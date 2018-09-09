package amqp

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrSessionClosed is propagated to Sender/Receivers
	// when Session.Close() is called.
	ErrSessionClosed = errors.New("amqp: session closed")

	// ErrLinkClosed returned by send and receive operations when
	// Sender.Close() or Receiver.Close() are called.
	ErrLinkClosed = errors.New("amqp: link closed")
)

// Client is an AMQP client connection.
type Client struct {
	amqpConn *amqpConnection
}

// Dial connects to an AMQP server.
//
// If the addr includes a scheme, it must be "amqp" or "amqps".
// If no port is provided, 5672 will be used for "amqp" and 5671 for "amqps".
//
// If username and password information is not empty it's used as SASL PLAIN
// credentials, equal to passing ConnSASLPlain option.
func Dial(theAddress string, opts ...ConnOption) (*Client, error) {
	destURL, err := url.Parse(theAddress)
	if err != nil {
		return nil, err
	}
	host, port, err := net.SplitHostPort(destURL.Host)
	if err != nil {
		host = destURL.Host
		port = "5672" // use default port values if parse fails
		if destURL.Scheme == "amqps" {
			port = "5671"
		}
	}

	// prepend SASL credentials when the user/pass segment is not empty
	if destURL.User != nil {
		pass, _ := destURL.User.Password()
		opts = append([]ConnOption{
			ConnSASLPlain(destURL.User.Username(), pass),
		}, opts...)
	}

	// append default options so user specified can overwrite
	opts = append([]ConnOption{
		ConnServerHostname(host),
	}, opts...)

	myAmqpConn, err := newAmqpConn(nil, opts...)
	if err != nil {
		return nil, err
	}
	switch destURL.Scheme {
	case "amqp", "":
		myAmqpConn.networkConnection, err = net.Dial("tcp", host+":"+port)
	case "amqps":
		myAmqpConn.initTLSConfig()
		myAmqpConn.tlsNegotiation = false
		myAmqpConn.networkConnection, err = tls.Dial("tcp", host+":"+port, myAmqpConn.tlsConfig)
	default:
		return nil, errorErrorf("unsupported scheme %q", destURL.Scheme)
	}
	if err != nil {
		return nil, err
	}
	err = myAmqpConn.start()
	return &Client{amqpConn: myAmqpConn}, err
}

// New establishes an AMQP client connection over conn.
func New(theNetConn net.Conn, opts ...ConnOption) (*Client, error) {
	myAmqpConn, err := newAmqpConn(theNetConn, opts...)
	if err != nil {
		return nil, err
	}
	err = myAmqpConn.start()
	return &Client{amqpConn: myAmqpConn}, err
}

// Close disconnects the connection.
func (theClient *Client) Close() error {
	return theClient.amqpConn.Close()
}

// NewSession opens a new AMQP session to the server.
func (theClient *Client) NewSession(opts ...SessionOption) (*Session, error) {
	// get a session allocated by Client.mux
	var sessionResponse newSessionResponse
	select {
	case <-theClient.amqpConn.done:
		return nil, theClient.amqpConn.getErr()
	case sessionResponse = <-theClient.amqpConn.newSession:
	}

	if sessionResponse.err != nil {
		return nil, sessionResponse.err
	}
	mySession := sessionResponse.session

	for _, opt := range opts {
		err := opt(mySession)
		if err != nil {
			_ = mySession.Close(context.Background()) // deallocate session on error
			return nil, err
		}
	}

	// send Begin to server
	beginRxFrame := &performBegin{
		NextOutgoingID: 0,
		IncomingWindow: mySession.incomingWindow,
		OutgoingWindow: mySession.outgoingWindow,
		HandleMax:      mySession.handleMax,
	}
	debug(1, "TX: %s", beginRxFrame)
	mySession.txFrame(beginRxFrame, nil)

	// TODO(eking) Send attach for sender

	// Send attach for receiver

	// Send flow message

	// wait for response
	var myRxFrame frame
	select {
	case <-theClient.amqpConn.done:
		return nil, theClient.amqpConn.getErr()
	case myRxFrame = <-mySession.inFrameChan:
	}
	debug(1, "RX: %s", myRxFrame.body)

	beginRxFrame, ok := myRxFrame.body.(*performBegin)
	if !ok {
		_ = mySession.Close(context.Background()) // deallocate session on error
		return nil, errorErrorf("unexpected begin response: %+v", myRxFrame.body)
	}

	// start Session multiplexor
	go mySession.mux(beginRxFrame)

	return mySession, nil
}

// Default session options
const (
	DefaultMaxLinks = 4294967296
	DefaultWindow   = 100
)

// SessionOption is an function for configuring an AMQP session.
type SessionOption func(*Session) error

// SessionIncomingWindow sets the maximum number of unacknowledged
// transfer frames the server can send.
func SessionIncomingWindow(window uint32) SessionOption {
	return func(aSession *Session) error {
		aSession.incomingWindow = window
		return nil
	}
}

// SessionOutgoingWindow sets the maximum number of unacknowledged
// transfer frames the client can send.
func SessionOutgoingWindow(window uint32) SessionOption {
	return func(aSession *Session) error {
		aSession.outgoingWindow = window
		return nil
	}
}

// SessionMaxLinks sets the maximum number of links (Senders/Receivers)
// allowed on the session.
//
// n must be in the range 1 to 4294967296.
//
// Default: 4294967296.
func SessionMaxLinks(maxLinks int) SessionOption {
	return func(aSession *Session) error {
		if maxLinks < 1 {
			return errorNew("max sessions cannot be less than 1")
		}
		if int64(maxLinks) > 4294967296 {
			return errorNew("max sessions cannot be greater than 4294967296")
		}
		aSession.handleMax = uint32(maxLinks - 1)
		return nil
	}
}

// Session is an AMQP session.
//
// A session multiplexes Receivers.
type Session struct {
	channel           uint16                // session's local channel
	remoteChannel     uint16                // session's remote channel, owned by conn.mux
	theAmqpConnection *amqpConnection       // underlying conn
	inFrameChan       chan frame            // frames destined for this session are sent on this chan by conn.mux
	outSvrFrameChan   chan frameBody        // non-transfer frames to be sent; session must track disposition
	outDataFrameChan  chan *performTransfer // transfer frames to be sent; session must track disposition

	// flow control
	incomingWindow uint32
	outgoingWindow uint32

	handleMax        uint32
	allocateHandle   chan *link // link handles are allocated by sending a link on this channel, nil is sent on link.rx once allocated
	deallocateHandle chan *link // link handles are deallocated by sending a link on this channel

	nextDeliveryID uint32 // atomically accessed sequence for deliveryIDs

	// used for gracefully closing link
	close     chan struct{}
	closeOnce sync.Once
	done      chan struct{}
	err       error
}

func newSession(theAmqpConnection *amqpConnection, channel uint16) *Session {
	return &Session{
		theAmqpConnection: theAmqpConnection,
		channel:           channel,
		inFrameChan:       make(chan frame),
		outSvrFrameChan:   make(chan frameBody),
		outDataFrameChan:  make(chan *performTransfer),
		incomingWindow:    DefaultWindow,
		outgoingWindow:    DefaultWindow,
		handleMax:         DefaultMaxLinks - 1,
		allocateHandle:    make(chan *link),
		deallocateHandle:  make(chan *link),
		close:             make(chan struct{}),
		done:              make(chan struct{}),
	}
}

// Close gracefully closes the session.
//
// If ctx expires while waiting for servers response, ctx.Err() will be returned.
// The session will continue to wait for the response until the Client is closed.
func (theSession *Session) Close(ctx context.Context) error {
	theSession.closeOnce.Do(func() { close(theSession.close) })
	select {
	case <-theSession.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	if theSession.err == ErrSessionClosed {
		return nil
	}
	return theSession.err
}

// txFrame sends a frame to the connWriter
func (theSession *Session) txFrame(p frameBody, done chan deliveryState) error {
	return theSession.theAmqpConnection.wantWriteFrame(frame{
		type_:   frameTypeAMQP,
		channel: theSession.channel,
		body:    p,
		done:    done,
	})
}

// randBytes returns a base64 encoded string of n bytes.
//
// A new random source is created to avoid any issues with seeding
// of the global source.
func randString(n int) string {
	localRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, n)
	localRand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

// NewReceiver opens a new receiver link on the session.
func (theSession *Session) NewReceiver(opts ...LinkOption) (*Receiver, error) {
	myReceiver := &Receiver{
		batching:    DefaultLinkBatching,
		batchMaxAge: DefaultLinkBatchMaxAge,
		maxCredit:   DefaultLinkCredit,
	}

	myReceiverLink, err := attachLink(theSession, myReceiver, opts)
	if err != nil {
		return nil, err
	}

	myReceiver.link = myReceiverLink

	// batching is just extra overhead when maxCredits == 1
	if myReceiver.maxCredit == 1 {
		myReceiver.batching = false
	}

	// create dispositions channel and start dispositionBatcher if batching enabled
	if myReceiver.batching {
		// buffer dispositions chan to prevent disposition sends from blocking
		myReceiver.dispositions = make(chan messageDisposition, myReceiver.maxCredit)
		go myReceiver.dispositionBatcher()
	}

	return myReceiver, nil
}

// Sender sends messages on a single AMQP link.
type Sender struct {
	link *link

	mu              sync.Mutex // protects buf and nextDeliveryTag
	buf             buffer
	nextDeliveryTag uint64
}

// Send sends a Message.
//
// Blocks until the message is sent, ctx completes, or an error occurs.
//
// Send is safe for concurrent use. Since only a single message can be
// sent on a link at a time, this is most useful when settlement confirmation
// has been requested (receiver settle mode is "Second"). In this case,
// additional messages can be sent while the current goroutine is waiting
// for the confirmation.
func (theSender *Sender) Send(ctx context.Context, msg *Message) error {
	done, err := theSender.send(ctx, msg)
	if err != nil {
		return err
	}

	// wait for transfer to be confirmed
	select {
	case state := <-done:
		if state, ok := state.(*stateRejected); ok {
			return state.Error
		}
		return nil
	case <-theSender.link.done:
		return theSender.link.err
	case <-ctx.Done():
		return errorWrapf(ctx.Err(), "awaiting send")
	}
}

// send is separated from Send so that the mutex unlock can be deferred without
// locking the transfer confirmation that happens in Send.
func (theSender *Sender) send(ctx context.Context, msg *Message) (chan deliveryState, error) {
	theSender.mu.Lock()
	defer theSender.mu.Unlock()

	theSender.buf.reset()
	err := msg.marshal(&theSender.buf)
	if err != nil {
		return nil, err
	}

	if theSender.link.maxMessageSize != 0 && uint64(theSender.buf.len()) > theSender.link.maxMessageSize {
		return nil, errorErrorf("encoded message size exceeds max of %d", theSender.link.maxMessageSize)
	}

	var (
		maxPayloadSize = int64(theSender.link.session.theAmqpConnection.peerMaxFrameSize) - maxTransferFrameHeader
		sndSettleMode  = theSender.link.senderSettleMode
		rcvSettleMode  = theSender.link.receiverSettleMode
		senderSettled  = sndSettleMode != nil && *sndSettleMode == ModeSettled
		deliveryID     = atomic.AddUint32(&theSender.link.session.nextDeliveryID, 1)
	)

	// use uint64 encoded as []byte as deliveryTag
	deliveryTag := make([]byte, 8)
	binary.BigEndian.PutUint64(deliveryTag, theSender.nextDeliveryTag)
	theSender.nextDeliveryTag++

	fr := performTransfer{
		Handle:        theSender.link.handle,
		DeliveryID:    &deliveryID,
		DeliveryTag:   deliveryTag,
		MessageFormat: &msg.Format,
		More:          theSender.buf.len() > 0,
	}

	for fr.More {
		buf, _ := theSender.buf.next(maxPayloadSize)
		fr.Payload = append([]byte(nil), buf...)
		fr.More = theSender.buf.len() > 0
		if !fr.More {
			// mark final transfer as settled when sender mode is settled
			fr.Settled = senderSettled

			// set done on last frame to be closed after network transmission
			//
			// If confirmSettlement is true (ReceiverSettleMode == "second"),
			// Session.mux will intercept the done channel and close it when the
			// receiver has confirmed settlement instead of on net transmit.
			fr.done = make(chan deliveryState, 1)
			fr.confirmSettlement = rcvSettleMode != nil && *rcvSettleMode == ModeSecond
		}

		select {
		case theSender.link.outTransferFrameChan <- fr:
		case <-theSender.link.done:
			return nil, theSender.link.err
		case <-ctx.Done():
			return nil, errorWrapf(ctx.Err(), "awaiting send")
		}

		// clear values that are only required on first message
		fr.DeliveryID = nil
		fr.DeliveryTag = nil
		fr.MessageFormat = nil
	}

	return fr.done, nil
}

// Address returns the link's address.
func (theSender *Sender) Address() string {
	if theSender.link.target == nil {
		return ""
	}
	return theSender.link.target.Address
}

// Close closes the Sender and AMQP link.
func (theSender *Sender) Close(ctx context.Context) error {
	return theSender.link.Close(ctx)
}

// NewSender opens a new sender link on the session.
func (theSession *Session) NewSender(opts ...LinkOption) (*Sender, error) {
	mySessionLink, err := attachLink(theSession, nil, opts)
	if err != nil {
		return nil, err
	}

	return &Sender{link: mySessionLink}, nil
}

func (theSender *Session) mux(remoteBegin *performBegin) {
	defer close(theSender.done)

	var (
		links       = make(map[uint32]*link)            // mapping of remote handles to links
		linksByName = make(map[string]*link)            // maping of names to links
		handles     = &bitmap{max: theSender.handleMax} // allocated handles

		handlesByDeliveryID       = make(map[uint32]uint32) // mapping of deliveryIDs to handles
		deliveryIDByHandle        = make(map[uint32]uint32) // mapping of handles to latest deliveryID
		handlesByRemoteDeliveryID = make(map[uint32]uint32) // mapping of remote deliveryID to handles

		settlementByDeliveryID = make(map[uint32]chan deliveryState)

		// flow control values
		nextOutgoingID       uint32
		nextIncomingID       = uint32(0)
		remoteIncomingWindow = uint32(0)
		remoteOutgoingWindow = uint32(0)
	)
	if remoteBegin != nil {
		nextIncomingID = remoteBegin.NextOutgoingID
		remoteIncomingWindow = remoteBegin.IncomingWindow
		remoteOutgoingWindow = remoteBegin.OutgoingWindow
	}

	for {
		txTransfer := theSender.outDataFrameChan
		// disable txTransfer if flow control windows have been exceeded
		if remoteIncomingWindow == 0 || theSender.outgoingWindow == 0 {
			txTransfer = nil
		}

		select {
		// conn has completed, exit
		case <-theSender.theAmqpConnection.done:
			theSender.err = theSender.theAmqpConnection.getErr()
			return

		// session is being closed by user
		case <-theSender.close:
			theSender.txFrame(&performEnd{}, nil)

			// discard frames until End is received or conn closed
		EndLoop:
			for {
				select {
				case inFrame := <-theSender.inFrameChan:
					_, ok := inFrame.body.(*performEnd)
					if ok {
						break EndLoop
					}
				case <-theSender.theAmqpConnection.done:
					theSender.err = theSender.theAmqpConnection.getErr()
					return
				}
			}

			// release session
			select {
			case theSender.theAmqpConnection.delSession <- theSender:
				theSender.err = ErrSessionClosed
			case <-theSender.theAmqpConnection.done:
				theSender.err = theSender.theAmqpConnection.getErr()
			}
			return

		// handle allocation request
		case senderLink := <-theSender.allocateHandle:
			next, ok := handles.next()
			if !ok {
				senderLink.err = errorErrorf("reached session handle max (%d)", theSender.handleMax)
				senderLink.inFrameChan <- nil
				continue
			}

			senderLink.handle = next                  // allocate handle to the link
			linksByName[senderLink.name] = senderLink // add to mapping
			senderLink.inFrameChan <- nil             // send nil on channel to indicate allocation complete

		// handle deallocation request
		case senderLink := <-theSender.deallocateHandle:
			delete(links, senderLink.remoteHandle)
			delete(deliveryIDByHandle, senderLink.handle)
			handles.remove(senderLink.handle)
			close(senderLink.inFrameChan) // close channel to indicate deallocation

		// incoming frame for link
		case inFrame := <-theSender.inFrameChan:
			debug(1, "RX(Session): %s", inFrame.body)

			switch body := inFrame.body.(type) {
			// Disposition frames can reference transfers from more than one
			// link. Send this frame to all of them.
			case *performDisposition:
				start := body.First
				end := start
				if body.Last != nil {
					end = *body.Last
				}
				for deliveryID := start; deliveryID <= end; deliveryID++ {
					handles := handlesByDeliveryID
					if body.Role == roleSender {
						handles = handlesByRemoteDeliveryID
					}

					handle, ok := handles[deliveryID]
					if !ok {
						continue
					}
					delete(handles, deliveryID)

					if body.Settled && body.Role == roleReceiver {
						// check if settlement confirmation was requested, if so
						// confirm by closing channel
						if done, ok := settlementByDeliveryID[deliveryID]; ok {
							delete(settlementByDeliveryID, deliveryID)
							select {
							case done <- body.State:
							default:
							}
							close(done)
						}
					}

					link, ok := links[handle]
					if !ok {
						continue
					}

					theSender.muxFrameToLink(link, inFrame.body)
				}
				continue
			case *performFlow:
				if body.NextIncomingID == nil {
					// This is a protocol error:
					//       "[...] MUST be set if the peer has received
					//        the begin frame for the session"
					theSender.txFrame(&performEnd{
						Error: &Error{
							Condition:   ErrorNotAllowed,
							Description: "next-incoming-id not set after session established",
						},
					}, nil)
					theSender.err = errors.New("protocol error: received flow without next-incoming-id after session established")
					return
				}

				// "When the endpoint receives a flow frame from its peer,
				// it MUST update the next-incoming-id directly from the
				// next-outgoing-id of the frame, and it MUST update the
				// remote-outgoing-window directly from the outgoing-window
				// of the frame."
				nextIncomingID = body.NextOutgoingID
				remoteOutgoingWindow = body.OutgoingWindow

				// "The remote-incoming-window is computed as follows:
				//
				// next-incoming-id(flow) + incoming-window(flow) - next-outgoing-id(endpoint)
				//
				// If the next-incoming-id field of the flow frame is not set, then remote-incoming-window is computed as follows:
				//
				// initial-outgoing-id(endpoint) + incoming-window(flow) - next-outgoing-id(endpoint)"
				remoteIncomingWindow = body.IncomingWindow - nextOutgoingID
				remoteIncomingWindow += *body.NextIncomingID

				// Send to link if handle is set
				if body.Handle != nil {
					link, ok := links[*body.Handle]
					if !ok {
						continue
					}

					theSender.muxFrameToLink(link, inFrame.body)
					continue
				}

				if body.Echo {
					niID := nextIncomingID
					resp := &performFlow{
						NextIncomingID: &niID,
						IncomingWindow: theSender.incomingWindow,
						NextOutgoingID: nextOutgoingID,
						OutgoingWindow: theSender.outgoingWindow,
					}
					debug(1, "TX: %s", resp)
					theSender.txFrame(resp, nil)
				}

			case *performAttach:
				// On Attach response link should be looked up by name, then added
				// to the links map with the remote's handle contained in this
				// attach frame.
				link, linkOk := linksByName[body.Name]
				if !linkOk {
					break
				}
				delete(linksByName, body.Name) // name no longer needed

				link.remoteHandle = body.Handle
				links[link.remoteHandle] = link

				theSender.muxFrameToLink(link, inFrame.body)

			case *performTransfer:
				// "Upon receiving a transfer, the receiving endpoint will
				// increment the next-incoming-id to match the implicit
				// transfer-id of the incoming transfer plus one, as well
				// as decrementing the remote-outgoing-window, and MAY
				// (depending on policy) decrement its incoming-window."
				nextIncomingID++
				remoteOutgoingWindow--
				link, ok := links[body.Handle]
				if !ok {
					continue
				}

				select {
				case <-theSender.theAmqpConnection.done:
				case link.inFrameChan <- inFrame.body:
				}

				// if this message is received unsettled and link rcv-settle-mode == second, add to handlesByRemoteDeliveryID
				if !body.Settled && body.DeliveryID != nil && link.receiverSettleMode != nil && *link.receiverSettleMode == ModeSecond {
					handlesByRemoteDeliveryID[*body.DeliveryID] = body.Handle
				}

				// Update peer's outgoing window if half has been consumed.
				if remoteOutgoingWindow < theSender.incomingWindow/2 {
					nID := nextIncomingID
					flow := &performFlow{
						NextIncomingID: &nID,
						IncomingWindow: theSender.incomingWindow,
						NextOutgoingID: nextOutgoingID,
						OutgoingWindow: theSender.outgoingWindow,
					}
					debug(1, "TX(Session): %s", flow)
					theSender.txFrame(flow, nil)
					remoteOutgoingWindow = theSender.incomingWindow
				}

			case *performDetach:
				link, ok := links[body.Handle]
				if !ok {
					continue
				}
				theSender.muxFrameToLink(link, inFrame.body)

			case *performEnd:
				theSender.txFrame(&performEnd{}, nil)
				theSender.err = errorErrorf("session ended by server: %s", body.Error)
				return

			default:
				fmt.Printf("Unexpected frame: %s\n", body)
			}

		case inTransferFrame := <-txTransfer:

			// record current delivery ID
			var deliveryID uint32
			if inTransferFrame.DeliveryID != nil {
				deliveryID = *inTransferFrame.DeliveryID
				deliveryIDByHandle[inTransferFrame.Handle] = deliveryID

				// add to handleByDeliveryID if not sender-settled
				if !inTransferFrame.Settled {
					handlesByDeliveryID[deliveryID] = inTransferFrame.Handle
				}
			} else {
				// if fr.DeliveryID is nil it must have been added
				// to deliveryIDByHandle already
				deliveryID = deliveryIDByHandle[inTransferFrame.Handle]
			}

			// frame has been sender-settled, remove from map
			if inTransferFrame.Settled {
				delete(handlesByDeliveryID, deliveryID)
			}

			// if confirmSettlement requested, add done chan to map
			// and clear from frame so conn doesn't close it.
			if inTransferFrame.confirmSettlement && inTransferFrame.done != nil {
				settlementByDeliveryID[deliveryID] = inTransferFrame.done
				inTransferFrame.done = nil
			}

			debug(2, "TX(Session): %s", inTransferFrame)
			theSender.txFrame(inTransferFrame, inTransferFrame.done)

			// "Upon sending a transfer, the sending endpoint will increment
			// its next-outgoing-id, decrement its remote-incoming-window,
			// and MAY (depending on policy) decrement its outgoing-window."
			nextOutgoingID++
			remoteIncomingWindow--

		case fr := <-theSender.outSvrFrameChan:
			switch fr := fr.(type) {
			case *performFlow:
				niID := nextIncomingID
				fr.NextIncomingID = &niID
				fr.IncomingWindow = theSender.incomingWindow
				fr.NextOutgoingID = nextOutgoingID
				fr.OutgoingWindow = theSender.outgoingWindow
				debug(1, "TX(Session): %s", fr)
				theSender.txFrame(fr, nil)
				remoteOutgoingWindow = theSender.incomingWindow
			case *performTransfer:
				panic("transfer frames must use txTransfer")
			default:
				debug(1, "TX(Session): %s", fr)
				theSender.txFrame(fr, nil)
			}
		}
	}
}

func (theSession *Session) muxFrameToLink(theLink *link, theFrameBody frameBody) {
	select {
	case theLink.inFrameChan <- theFrameBody:
	case <-theLink.done:
	case <-theSession.theAmqpConnection.done:
	}
}

// DetachError is returned by a link (Receiver/Sender) when a detach frame is received.
//
// RemoteError will be nil if the link was detached gracefully.
type DetachError struct {
	RemoteError *Error
}

func (e *DetachError) Error() string {
	return fmt.Sprintf("link detached, reason: %+v", e.RemoteError)
}

// Default link options
const (
	DefaultLinkCredit      = 1
	DefaultLinkBatching    = true
	DefaultLinkBatchMaxAge = 5 * time.Second
)

// link is a unidirectional route.
//
// May be used for sending or receiving.
type link struct {
	name                 string               // our name
	handle               uint32               // our handle
	remoteHandle         uint32               // remote's handle
	dynamicAddr          bool                 // request a dynamic link address from the server
	inFrameChan          chan frameBody       // sessions sends frames for this link on this channel
	outTransferFrameChan chan performTransfer // sender uses to send transfer frames
	closeOnce            sync.Once            // closeOnce protects close from being closed multiple times
	close                chan struct{}        // close signals the mux to shutdown
	done                 chan struct{}        // done is closed by mux/muxDetach when the link is fully detached
	detachErrorMu        sync.Mutex           // protects detachError
	detachError          *Error               // error to send to remote on detach, set by closeWithError
	session              *Session             // parent session
	receiver             *Receiver            // allows link options to modify Receiver
	source               *source
	target               *target
	properties           map[symbol]interface{} // additional properties sent upon link attach

	// "The delivery-count is initialized by the sender when a link endpoint is created,
	// and is incremented whenever a message is sent. Only the sender MAY independently
	// modify this field. The receiver's value is calculated based on the last known
	// value from the sender and any subsequent messages received on the link. Note that,
	// despite its name, the delivery-count is not a count but a sequence number
	// initialized at an arbitrary point by the sender."
	deliveryCount      uint32
	linkCredit         uint32 // maximum number of messages allowed between flow updates
	senderSettleMode   *SenderSettleMode
	receiverSettleMode *ReceiverSettleMode
	maxMessageSize     uint64
	detachReceived     bool
	err                error // err returned on Close()

	// message receiving
	paused        uint32        // atomically accessed; indicates that all link credits have been used by sender
	receiverReady chan struct{} // receiver sends on this when mux is paused to indicate it can handle more messages
	messages      chan Message  // used to send completed messages to receiver
	buf           buffer        // buffered bytes for current message
	more          bool          // if true, buf contains a partial message
	msg           Message       // current message being decoded
}

// attachLink is used by Receiver and Sender to create new links
func attachLink(theSession *Session, theReceiver *Receiver, opts []LinkOption) (*link, error) {
	mySessionLink, err := newLink(theSession, theReceiver, opts)
	if err != nil {
		return nil, err
	}

	isReceiver := theReceiver != nil

	// buffer rx to linkCredit so that conn.mux won't block
	// attempting to send to a slow reader
	if isReceiver {
		mySessionLink.inFrameChan = make(chan frameBody, mySessionLink.linkCredit)
	} else {
		mySessionLink.inFrameChan = make(chan frameBody, 1)
	}

	// request handle from Session.mux
	select {
	case <-theSession.done:
		return nil, theSession.err
	case theSession.allocateHandle <- mySessionLink:
	}

	// wait for handle allocation
	select {
	case <-theSession.done:
		return nil, theSession.err
	case <-mySessionLink.inFrameChan:
	}

	// check for link request error
	if mySessionLink.err != nil {
		return nil, mySessionLink.err
	}

	attachPerformative := &performAttach{
		Name:               mySessionLink.name,
		Handle:             mySessionLink.handle,
		ReceiverSettleMode: mySessionLink.receiverSettleMode,
		SenderSettleMode:   mySessionLink.senderSettleMode,
		MaxMessageSize:     mySessionLink.maxMessageSize,
		Source:             mySessionLink.source,
		Target:             mySessionLink.target,
		Properties:         mySessionLink.properties,
	}

	if isReceiver {
		attachPerformative.Role = roleReceiver
		if attachPerformative.Source == nil {
			attachPerformative.Source = new(source)
		}
		attachPerformative.Source.Dynamic = mySessionLink.dynamicAddr
	} else {
		attachPerformative.Role = roleSender
		if attachPerformative.Target == nil {
			attachPerformative.Target = new(target)
		}
		attachPerformative.Target.Dynamic = mySessionLink.dynamicAddr
	}

	// send Attach frame
	debug(1, "TX: %s", attachPerformative)
	theSession.txFrame(attachPerformative, nil)

	// wait for response
	var responseFrameBody frameBody
	select {
	case <-theSession.done:
		return nil, theSession.err
	case responseFrameBody = <-mySessionLink.inFrameChan:
	}
	debug(3, "RX: %s", responseFrameBody)
	resp, ok := responseFrameBody.(*performAttach)
	if !ok {
		return nil, errorErrorf("unexpected attach response: %#v", responseFrameBody)
	}

	if mySessionLink.maxMessageSize == 0 || resp.MaxMessageSize < mySessionLink.maxMessageSize {
		mySessionLink.maxMessageSize = resp.MaxMessageSize
	}

	if isReceiver {
		// if dynamic address requested, copy assigned name to address
		if mySessionLink.dynamicAddr && resp.Source != nil {
			mySessionLink.source.Address = resp.Source.Address
		}
		// deliveryCount is a sequence number, must initialize to sender's initial sequence number
		mySessionLink.deliveryCount = resp.InitialDeliveryCount
		// buffer receiver so that link.mux doesn't block
		mySessionLink.messages = make(chan Message, mySessionLink.receiver.maxCredit)
		if resp.SenderSettleMode != nil {
			mySessionLink.senderSettleMode = resp.SenderSettleMode
		}
	} else {
		// if dynamic address requested, copy assigned name to address
		if mySessionLink.dynamicAddr && resp.Target != nil {
			mySessionLink.target.Address = resp.Target.Address
		}
		mySessionLink.outTransferFrameChan = make(chan performTransfer)
		if resp.ReceiverSettleMode != nil {
			mySessionLink.receiverSettleMode = resp.ReceiverSettleMode
		}
	}

	go mySessionLink.mux()

	return mySessionLink, nil
}

func newLink(theSession *Session, theReceiver *Receiver, opts []LinkOption) (*link, error) {
	mySessionLink := &link{
		name:          randString(40),
		session:       theSession,
		receiver:      theReceiver,
		close:         make(chan struct{}),
		done:          make(chan struct{}),
		receiverReady: make(chan struct{}, 1),
	}

	// configure options
	for _, o := range opts {
		err := o(mySessionLink)
		if err != nil {
			return nil, err
		}
	}

	return mySessionLink, nil
}

func (theLink *link) mux() {
	defer theLink.muxDetach()

	var (
		isReceiver = theLink.receiver != nil
		isSender   = !isReceiver
	)

Loop:
	for {
		var outgoingTransfers chan performTransfer
		switch {
		// enable outgoing transfers case if sender and credits are available
		case isSender && theLink.linkCredit > 0:
			outgoingTransfers = theLink.outTransferFrameChan

		// if receiver && half maxCredits have been processed, send more credits
		case isReceiver && theLink.linkCredit+uint32(len(theLink.messages)) <= theLink.receiver.maxCredit/2:
			theLink.err = theLink.muxFlow()
			if theLink.err != nil {
				return
			}
			atomic.StoreUint32(&theLink.paused, 0)

		case isReceiver && theLink.linkCredit == 0:
			atomic.StoreUint32(&theLink.paused, 1)
		}

		select {
		// received frame
		case fr := <-theLink.inFrameChan:
			theLink.err = theLink.muxHandleFrame(fr)
			if theLink.err != nil {
				return
			}

		// send data
		case outTransferFrame := <-outgoingTransfers:
			debug(3, "TX(link): %s", outTransferFrame)

			// Ensure the session mux is not blocked
			for {
				select {
				case theLink.session.outDataFrameChan <- &outTransferFrame:
					// decrement link-credit after entire message transferred
					if !outTransferFrame.More {
						theLink.deliveryCount++
						theLink.linkCredit--
					}
					continue Loop
				case fr := <-theLink.inFrameChan:
					theLink.err = theLink.muxHandleFrame(fr)
					if theLink.err != nil {
						return
					}
				case <-theLink.close:
					theLink.err = ErrLinkClosed
					return
				case <-theLink.session.done:
					theLink.err = theLink.session.err
					return
				}
			}

		case <-theLink.receiverReady:
			continue
		case <-theLink.close:
			theLink.err = ErrLinkClosed
			return
		case <-theLink.session.done:
			theLink.err = theLink.session.err
			return
		}
	}
}

// muxFlow sends tr to the session mux.
func (theLink *link) muxFlow() error {
	// copy because sent by pointer below; prevent race
	var (
		linkCredit    = theLink.receiver.maxCredit - uint32(len(theLink.messages))
		deliveryCount = theLink.deliveryCount
	)

	flowPerformative := &performFlow{
		Handle:        &theLink.handle,
		DeliveryCount: &deliveryCount,
		LinkCredit:    &linkCredit, // max number of messages
	}
	debug(3, "TX: %s", flowPerformative)

	// Update credit. This must happen before entering loop below
	// because incoming messages handled while waiting to transmit
	// flow increment deliveryCount. This causes the credit to become
	// out of sync with the server.
	theLink.linkCredit = linkCredit

	// Ensure the session mux is not blocked
	for {
		select {
		case theLink.session.outSvrFrameChan <- flowPerformative:
			return nil
		case fr := <-theLink.inFrameChan:
			err := theLink.muxHandleFrame(fr)
			if err != nil {
				return err
			}
		case <-theLink.close:
			return ErrLinkClosed
		case <-theLink.session.done:
			return theLink.session.err
		}
	}
}

func (theLink *link) muxReceive(transferFrame performTransfer) error {
	// record the delivery ID and message format if this is
	// the first frame of the message
	if !theLink.more {
		if transferFrame.DeliveryID != nil {
			theLink.msg.deliveryID = *transferFrame.DeliveryID
		}

		if transferFrame.MessageFormat != nil {
			theLink.msg.Format = *transferFrame.MessageFormat
		}
	}

	// ensure maxMessageSize will not be exceeded
	if theLink.maxMessageSize != 0 && uint64(theLink.buf.len())+uint64(len(transferFrame.Payload)) > theLink.maxMessageSize {
		msg := fmt.Sprintf("received message larger than max size of %d", theLink.maxMessageSize)
		theLink.closeWithError(&Error{
			Condition:   ErrorMessageSizeExceeded,
			Description: msg,
		})
		return errorNew(msg)
	}

	// add the payload the the buffer
	theLink.buf.write(transferFrame.Payload)

	// mark as settled if at least one frame is settled
	theLink.msg.settled = theLink.msg.settled || transferFrame.Settled

	// save in-progress status
	theLink.more = transferFrame.More

	if transferFrame.More {
		return nil
	}

	// last frame in message
	err := theLink.msg.unmarshal(&theLink.buf)
	if err != nil {
		return err
	}

	// send to receiver, this should never block due to buffering
	// and flow control.
	theLink.messages <- theLink.msg

	// reset progress
	theLink.buf.reset()
	theLink.msg = Message{}

	// decrement link-credit after entire message received
	theLink.deliveryCount++
	theLink.linkCredit--

	return nil
}

// muxHandleFrame processes fr based on type.
func (theLink *link) muxHandleFrame(theFrameBody frameBody) error {
	var (
		isSender               = theLink.receiver == nil
		errOnRejectDisposition = isSender && (theLink.receiverSettleMode == nil || *theLink.receiverSettleMode == ModeFirst)
	)

	switch fr := theFrameBody.(type) {
	// message frame
	case *performTransfer:
		debug(3, "RX: %s", fr)
		if isSender {
			// Senders should never receive transfer frames, but handle it just in case.
			theLink.closeWithError(&Error{
				Condition:   ErrorNotAllowed,
				Description: "sender cannot process transfer frame",
			})
			return errorErrorf("sender received transfer frame")
		}

		return theLink.muxReceive(*fr)

	// flow control frame
	case *performFlow:
		debug(3, "RX: %s", fr)
		if isSender {
			linkCredit := *fr.LinkCredit - theLink.deliveryCount
			if fr.DeliveryCount != nil {
				// DeliveryCount can be nil if the receiver hasn't processed
				// the attach. That shouldn't be the case here, but it's
				// what ActiveMQ does.
				linkCredit += *fr.DeliveryCount
			}
			theLink.linkCredit = linkCredit
		}

		if !fr.Echo {
			return nil
		}

		var (
			// copy because sent by pointer below; prevent race
			linkCredit    = theLink.linkCredit
			deliveryCount = theLink.deliveryCount
		)

		// send flow
		resp := &performFlow{
			Handle:        &theLink.handle,
			DeliveryCount: &deliveryCount,
			LinkCredit:    &linkCredit, // max number of messages
		}
		debug(1, "TX: %s", resp)
		theLink.session.txFrame(resp, nil)

	// remote side is closing links
	case *performDetach:
		debug(1, "RX: %s", fr)
		// don't currently support link detach and reattach
		if !fr.Closed {
			return errorErrorf("non-closing detach not supported: %+v", fr)
		}

		// set detach received and close link
		theLink.detachReceived = true

		return errorWrapf(&DetachError{fr.Error}, "received detach frame")

	case *performDisposition:
		debug(3, "RX: %s", fr)

		// Unblock receivers waiting for message disposition
		if theLink.receiver != nil {
			theLink.receiver.inFlight.remove(fr.First, fr.Last, nil)
		}

		// If sending async and a message is rejected, cause a link error.
		//
		// This isn't ideal, but there isn't a clear better way to handle it.
		if fr, ok := fr.State.(*stateRejected); ok && errOnRejectDisposition {
			return fr.Error
		}

		if fr.Settled {
			return nil
		}

		resp := &performDisposition{
			Role:    roleSender,
			First:   fr.First,
			Last:    fr.Last,
			Settled: true,
		}
		debug(1, "TX: %s", resp)
		theLink.session.txFrame(resp, nil)

	default:
		debug(1, "RX: %s", fr)
		fmt.Printf("Unexpected frame: %s\n", fr)
	}

	return nil
}

// close closes and requests deletion of the link.
//
// No operations on link are valid after close.
//
// If ctx expires while waiting for servers response, ctx.Err() will be returned.
// The session will continue to wait for the response until the Session or Client
// is closed.
func (theLink *link) Close(ctx context.Context) error {
	theLink.closeOnce.Do(func() { close(theLink.close) })
	select {
	case <-theLink.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	if theLink.err == ErrLinkClosed {
		return nil
	}
	return theLink.err
}

func (theLink *link) closeWithError(de *Error) {
	theLink.closeOnce.Do(func() {
		theLink.detachErrorMu.Lock()
		theLink.detachError = de
		theLink.detachErrorMu.Unlock()
		close(theLink.close)
	})
}

func (theLink *link) muxDetach() {
	defer func() {
		// final cleanup and signaling

		// deallocate handle
		select {
		case theLink.session.deallocateHandle <- theLink:
		case <-theLink.session.done:
			if theLink.err == nil {
				theLink.err = theLink.session.err
			}
		}

		// signal other goroutines that link is done
		close(theLink.done)

		// unblock any in flight message dispositions
		if theLink.receiver != nil {
			theLink.receiver.inFlight.clear(theLink.err)
		}
	}()

	// "A peer closes a link by sending the detach frame with the
	// handle for the specified link, and the closed flag set to
	// true. The partner will destroy the corresponding link
	// endpoint, and reply with its own detach frame with the
	// closed flag set to true.
	//
	// Note that one peer MAY send a closing detach while its
	// partner is sending a non-closing detach. In this case,
	// the partner MUST signal that it has closed the link by
	// reattaching and then sending a closing detach."

	theLink.detachErrorMu.Lock()
	detachError := theLink.detachError
	theLink.detachErrorMu.Unlock()

	fr := &performDetach{
		Handle: theLink.handle,
		Closed: true,
		Error:  detachError,
	}

Loop:
	for {
		select {
		case theLink.session.outSvrFrameChan <- fr:
			// after sending the detach frame, break the read loop
			break Loop
		case fr := <-theLink.inFrameChan:
			// discard incoming frames to avoid blocking session.mux
			if fr, ok := fr.(*performDetach); ok && fr.Closed {
				theLink.detachReceived = true
			}
		case <-theLink.session.done:
			if theLink.err == nil {
				theLink.err = theLink.session.err
			}
			return
		}
	}

	// don't wait for remote to detach when already
	// received or closing due to error
	if theLink.detachReceived || detachError != nil {
		return
	}

	for {
		select {
		// read from link until detach with Close == true is received,
		// other frames are discarded.
		case fr := <-theLink.inFrameChan:
			if fr, ok := fr.(*performDetach); ok && fr.Closed {
				return
			}

		// connection has ended
		case <-theLink.session.done:
			if theLink.err == nil {
				theLink.err = theLink.session.err
			}
			return
		}
	}
}

// LinkOption is a function for configuring an AMQP link.
//
// A link may be a Sender or a Receiver.
type LinkOption func(*link) error

// LinkAddress sets the link address.
//
// For a Receiver this configures the source address.
// For a Sender this configures the target address.
//
// Deprecated: use LinkSourceAddress or LinkTargetAddress instead.
func LinkAddress(source string) LinkOption {
	return func(theLink *link) error {
		if theLink.receiver != nil {
			return LinkSourceAddress(source)(theLink)
		}
		return LinkTargetAddress(source)(theLink)
	}
}

// LinkProperty sets an entry in the link properties map sent to the server.
//
// This option can be used multiple times.
func LinkProperty(key, value string) LinkOption {
	return linkProperty(key, value)
}

// LinkPropertyInt64 sets an entry in the link properties map sent to the server.
//
// This option can be used multiple times.
func LinkPropertyInt64(key string, value int64) LinkOption {
	return linkProperty(key, value)
}

func linkProperty(key string, value interface{}) LinkOption {
	return func(theLink *link) error {
		if key == "" {
			return errorNew("link property key must not be empty")
		}
		if theLink.properties == nil {
			theLink.properties = make(map[symbol]interface{})
		}
		theLink.properties[symbol(key)] = value
		return nil
	}
}

// LinkSourceAddress sets the source address.
func LinkSourceAddress(addr string) LinkOption {
	return func(theLink *link) error {
		if theLink.source == nil {
			theLink.source = new(source)
		}
		theLink.source.Address = addr
		return nil
	}
}

// LinkTargetAddress sets the target address.
func LinkTargetAddress(addr string) LinkOption {
	return func(theLink *link) error {
		if theLink.target == nil {
			theLink.target = new(target)
		}
		theLink.target.Address = addr
		return nil
	}
}

// LinkAddressDynamic requests a dynamically created address from the server.
func LinkAddressDynamic() LinkOption {
	return func(theLink *link) error {
		theLink.dynamicAddr = true
		return nil
	}
}

// LinkCredit specifies the maximum number of unacknowledged messages
// the sender can transmit.
func LinkCredit(credit uint32) LinkOption {
	return func(theLink *link) error {
		if theLink.receiver == nil {
			return errorNew("LinkCredit is not valid for Sender")
		}

		theLink.receiver.maxCredit = credit
		return nil
	}
}

// LinkBatching toggles batching of message disposition.
//
// When enabled, accepting a message does not send the disposition
// to the server until the batch is equal to link credit or the
// batch max age expires.
func LinkBatching(enable bool) LinkOption {
	return func(theLink *link) error {
		theLink.receiver.batching = enable
		return nil
	}
}

// LinkBatchMaxAge sets the maximum time between the start
// of a disposition batch and sending the batch to the server.
func LinkBatchMaxAge(d time.Duration) LinkOption {
	return func(theLink *link) error {
		theLink.receiver.batchMaxAge = d
		return nil
	}
}

// LinkSenderSettle sets the sender settlement mode.
//
// When the Link is the Receiver, this is a request to the remote
// server.
//
// When the Link is the Sender, this is the actual settlement mode.
func LinkSenderSettle(mode SenderSettleMode) LinkOption {
	return func(theLink *link) error {
		if mode > ModeMixed {
			return errorErrorf("invalid SenderSettlementMode %d", mode)
		}
		theLink.senderSettleMode = &mode
		return nil
	}
}

// LinkReceiverSettle sets the receiver settlement mode.
//
// When the Link is the Sender, this is a request to the remote
// server.
//
// When the Link is the Receiver, this is the actual settlement mode.
func LinkReceiverSettle(mode ReceiverSettleMode) LinkOption {
	return func(theLink *link) error {
		if mode > ModeSecond {
			return errorErrorf("invalid ReceiverSettlementMode %d", mode)
		}
		theLink.receiverSettleMode = &mode
		return nil
	}
}

// LinkSessionFilter sets a session filter (com.microsoft:session-filter) on the link source.
// This is used in Azure Service Bus to filter messages by session ID on a receiving link.
func LinkSessionFilter(sessionID string) LinkOption {
	// <descriptor name="com.microsoft:session-filter" code="00000013:7000000C"/>
	return linkSourceFilter("com.microsoft:session-filter", uint64(0x00000137000000C), sessionID)
}

// LinkSelectorFilter sets a selector filter (apache.org:selector-filter:string) on the link source.
func LinkSelectorFilter(filter string) LinkOption {
	// <descriptor name="apache.org:selector-filter:string" code="0x0000468C:0x00000004"/>
	return linkSourceFilter("apache.org:selector-filter:string", uint64(0x0000468C00000004), filter)
}

// linkSourceFilter sets a filter on the link source.
func linkSourceFilter(name string, code uint64, value string) LinkOption {
	nameSym := symbol(name)
	return func(theLink *link) error {
		if theLink.source == nil {
			theLink.source = new(source)
		}
		if theLink.source.Filter == nil {
			theLink.source.Filter = make(map[symbol]*describedType)
		}
		theLink.source.Filter[nameSym] = &describedType{
			descriptor: code,
			value:      value,
		}
		return nil
	}
}

// LinkMaxMessageSize sets the maximum message size that can
// be sent or received on the link.
//
// A size of zero indicates no limit.
//
// Default: 0.
func LinkMaxMessageSize(size uint64) LinkOption {
	return func(theLink *link) error {
		theLink.maxMessageSize = size
		return nil
	}
}

// Receiver receives messages on a single AMQP link.
type Receiver struct {
	link         *link                   // underlying link
	batching     bool                    // enable batching of message dispositions
	batchMaxAge  time.Duration           // maximum time between the start n batch and sending the batch to the server
	dispositions chan messageDisposition // message dispositions are sent on this channel when batching is enabled
	maxCredit    uint32                  // maximum allowed inflight messages
	inFlight     inFlight                // used to track message disposition when rcv-settle-mode == second
}

// Receive returns the next message from the sender.
//
// Blocks until a message is received, ctx completes, or an error occurs.
func (theReceiver *Receiver) Receive(ctx context.Context) (*Message, error) {
	if atomic.LoadUint32(&theReceiver.link.paused) == 1 {
		select {
		case theReceiver.link.receiverReady <- struct{}{}:
		default:
		}
	}

	// non-blocking receive to ensure buffered messages are
	// delivered regardless of whether the link has been closed.
	select {
	case msg := <-theReceiver.link.messages:
		msg.receiver = theReceiver
		return &msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// wait for the next message
	select {
	case msg := <-theReceiver.link.messages:
		msg.receiver = theReceiver
		return &msg, nil
	case <-theReceiver.link.done:
		return nil, theReceiver.link.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Address returns the link's address.
func (theReceiver *Receiver) Address() string {
	if theReceiver.link.source == nil {
		return ""
	}
	return theReceiver.link.source.Address
}

// Close closes the Receiver and AMQP link.
//
// If ctx expires while waiting for servers response, ctx.Err() will be returned.
// The session will continue to wait for the response until the Session or Client
// is closed.
func (theReceiver *Receiver) Close(ctx context.Context) error {
	return theReceiver.link.Close(ctx)
}

type messageDisposition struct {
	id    uint32
	state interface{}
}

func (theReceiver *Receiver) dispositionBatcher() {
	// batch operations:
	// Keep track of the first and last delivery ID, incrementing as
	// Accept() is called. After last-first == batchSize, send disposition.
	// If Reject()/Release() is called, send one disposition for previously
	// accepted, and one for the rejected/released message. If messages are
	// accepted out of order, send any existing batch and the current message.
	var (
		batchSize    = theReceiver.maxCredit
		batchStarted bool
		first        uint32
		last         uint32
	)

	// create an unstarted timer
	batchTimer := time.NewTimer(1 * time.Minute)
	batchTimer.Stop()
	defer batchTimer.Stop()

	for {
		select {
		case msgDis := <-theReceiver.dispositions:

			// not accepted or batch out of order
			_, isAccept := msgDis.state.(*stateAccepted)
			if !isAccept || (batchStarted && last+1 != msgDis.id) {
				// send the current batch, if any
				if batchStarted {
					lastCopy := last
					err := theReceiver.sendDisposition(first, &lastCopy, &stateAccepted{})
					if err != nil {
						theReceiver.inFlight.remove(first, &lastCopy, err)
					}
					batchStarted = false
				}

				// send the current message
				err := theReceiver.sendDisposition(msgDis.id, nil, msgDis.state)
				if err != nil {
					theReceiver.inFlight.remove(msgDis.id, nil, err)
				}
				continue
			}

			if batchStarted {
				// increment last
				last++
			} else {
				// start new batch
				batchStarted = true
				first = msgDis.id
				last = msgDis.id
				batchTimer.Reset(theReceiver.batchMaxAge)
			}

			// send batch if current size == batchSize
			if last-first+1 >= batchSize {
				lastCopy := last
				err := theReceiver.sendDisposition(first, &lastCopy, &stateAccepted{})
				if err != nil {
					theReceiver.inFlight.remove(first, &lastCopy, err)
				}
				batchStarted = false
				if !batchTimer.Stop() {
					<-batchTimer.C // batch timer must be drained if stop returns false
				}
			}

		// maxBatchAge elapsed, send batch
		case <-batchTimer.C:
			lastCopy := last
			err := theReceiver.sendDisposition(first, &lastCopy, &stateAccepted{})
			if err != nil {
				theReceiver.inFlight.remove(first, &lastCopy, err)
			}
			batchStarted = false
			batchTimer.Stop()

		case <-theReceiver.link.done:
			return
		}
	}
}

// sendDisposition sends a disposition frame to the peer
func (theReceiver *Receiver) sendDisposition(first uint32, last *uint32, state interface{}) error {
	fr := &performDisposition{
		Role:    roleReceiver,
		First:   first,
		Last:    last,
		Settled: theReceiver.link.receiverSettleMode == nil || *theReceiver.link.receiverSettleMode == ModeFirst,
		State:   state,
	}

	debug(1, "TX: %s", fr)
	return theReceiver.link.session.txFrame(fr, nil)
}

func (theReceiver *Receiver) messageDisposition(id uint32, state interface{}) error {
	var wait chan error
	if theReceiver.link.receiverSettleMode != nil && *theReceiver.link.receiverSettleMode == ModeSecond {
		wait = theReceiver.inFlight.add(id)
	}

	if theReceiver.batching {
		theReceiver.dispositions <- messageDisposition{id: id, state: state}
	} else {
		err := theReceiver.sendDisposition(id, nil, state)
		if err != nil {
			return err
		}
	}

	if wait == nil {
		return nil
	}

	return <-wait
}

// inFlight tracks in-flight message dispositions allowing receivers
// to block waiting for the server to respond when an appropriate
// settlement mode is configured.
type inFlight struct {
	mu sync.Mutex
	m  map[uint32]chan error
}

func (inFlightTracker *inFlight) add(id uint32) chan error {
	wait := make(chan error, 1)

	inFlightTracker.mu.Lock()
	if inFlightTracker.m == nil {
		inFlightTracker.m = map[uint32]chan error{id: wait}
	} else {
		inFlightTracker.m[id] = wait
	}
	inFlightTracker.mu.Unlock()

	return wait
}

func (inFlightTracker *inFlight) remove(first uint32, last *uint32, err error) {
	inFlightTracker.mu.Lock()

	if inFlightTracker.m == nil {
		inFlightTracker.mu.Unlock()
		return
	}

	ll := first
	if last != nil {
		ll = *last
	}

	for i := first; i <= ll; i++ {
		wait, ok := inFlightTracker.m[i]
		if ok {
			wait <- err
			delete(inFlightTracker.m, i)
		}
	}

	inFlightTracker.mu.Unlock()
}

func (inFlightTracker *inFlight) clear(err error) {
	inFlightTracker.mu.Lock()
	for id, wait := range inFlightTracker.m {
		wait <- err
		delete(inFlightTracker.m, id)
	}
	inFlightTracker.mu.Unlock()
}

const maxTransferFrameHeader = 66 // determined by calcMaxTransferFrameHeader

func calcMaxTransferFrameHeader() int {
	var buf buffer

	maxUint32 := uint32(math.MaxUint32)
	receiverSettleMode := ReceiverSettleMode(0)
	err := writeFrame(&buf, frame{
		type_:   frameTypeAMQP,
		channel: math.MaxUint16,
		body: &performTransfer{
			Handle:             maxUint32,
			DeliveryID:         &maxUint32,
			DeliveryTag:        bytes.Repeat([]byte{'a'}, 32),
			MessageFormat:      &maxUint32,
			Settled:            true,
			More:               true,
			ReceiverSettleMode: &receiverSettleMode,
			State:              nil, // TODO: determine whether state should be included in size
			Resume:             true,
			Aborted:            true,
			Batchable:          true,
			// Payload omitted as it is appended directly without any header
		},
	})
	if err != nil {
		panic(err)
	}

	return buf.len()
}
