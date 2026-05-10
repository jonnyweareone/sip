// SONIQ Persistent Endpoint Writer
//
// Treats the phone's TLS registration connection as a persistent bidirectional
// endpoint. All outbound SIP messages (INVITE, ACK, BYE, CANCEL, NOTIFY) go
// directly through the phone's existing TLS socket — no routing, no pool
// confusion, no private IP dialing.
//
// The phone connected to us. We write back on that same socket. Period.

package sip

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/livekit/protocol/logger"
	"github.com/livekit/sipgo/sip"
	"github.com/livekit/sipgo/transport"
	goredis "github.com/redis/go-redis/v9"

	sipgo "github.com/livekit/sipgo"
	"github.com/livekit/sip/pkg/config"
)

// EndpointWriter writes SIP messages to registered phones via their
// persistent TLS registration connections.
type EndpointWriter struct {
	log       logger.Logger
	redis     goredis.UniversalClient
	srv       *sipgo.Server // sipgo server — owns the TLS listener and connection pool
	cli       *sipgo.Client // sipgo client — for transaction-based INVITE/ACK/BYE
	mu        sync.RWMutex
	// Pending INVITE calls — map callID → PendingCall
	pending   map[string]*PendingCall
	pendingMu sync.Mutex
}

// PendingCall tracks an outbound INVITE sent via the persistent TLS connection.
type PendingCall struct {
	CallID       string
	Identity     string
	NatAddr      string
	FromTag      string
	ToTag        string  // populated from 180/200 response
	LocalSDP     string  // our SDP offer
	RemoteSDP    string  // phone's SDP answer (from 200 OK)
	RTPPort      int     // our offered RTP port
	State        string  // "invited", "ringing", "answered", "bye"
	Conf         *config.SONIQConfig
	AnsweredChan chan struct{} // closed when 200 OK received
}

func NewEndpointWriter(log logger.Logger, rc goredis.UniversalClient) *EndpointWriter {
	return &EndpointWriter{
		log:     log.WithValues("component", "soniq-endpoint-writer"),
		redis:   rc,
		pending: make(map[string]*PendingCall),
	}
}

// SetServer wires the sipgo server reference (called after server starts).
func (ew *EndpointWriter) SetServer(srv *sipgo.Server) {
	ew.mu.Lock()
	defer ew.mu.Unlock()
	ew.srv = srv
}

// SetClient wires the sipgo client for transaction-based requests (INVITE).
func (ew *EndpointWriter) SetClient(cli *sipgo.Client) {
	ew.mu.Lock()
	defer ew.mu.Unlock()
	ew.cli = cli
}

// GetConnection returns the phone's persistent TLS connection from the
// transport pool. The connection is keyed by the phone's NAT address
// (RemoteAddr as seen by the TLS listener when the phone registered).
func (ew *EndpointWriter) GetConnection(ctx context.Context, identity string) (transport.Connection, string, error) {
	// 1. Look up NAT address from Redis
	key := redisEndpointPrefix + identity
	fields, err := ew.redis.HGetAll(ctx, key).Result()
	if err != nil || len(fields) == 0 {
		return nil, "", fmt.Errorf("endpoint not registered: %s", identity)
	}

	natAddr := fields["nat_ip"] + ":" + fields["nat_port"]

	// 2. Get transport layer from sipgo server
	ew.mu.RLock()
	srv := ew.srv
	ew.mu.RUnlock()

	if srv == nil {
		return nil, natAddr, fmt.Errorf("sipgo server not available")
	}

	tl := srv.TransportLayer()
	if tl == nil {
		return nil, natAddr, fmt.Errorf("transport layer not available")
	}

	// 3. Get the connection from the TLS pool using NAT address
	conn, err := tl.GetConnection("tls", natAddr)
	if err != nil {
		return nil, natAddr, fmt.Errorf("no TLS connection for %s: %w", natAddr, err)
	}
	if conn == nil {
		return nil, natAddr, fmt.Errorf("TLS connection not found for %s (phone may have disconnected)", natAddr)
	}

	return conn, natAddr, nil
}

// WriteMsg sends a SIP message directly to a registered phone's TLS connection.
// This bypasses all sipgo client routing — writes straight to the socket.
func (ew *EndpointWriter) WriteMsg(ctx context.Context, identity string, msg sip.Message) error {
	conn, natAddr, err := ew.GetConnection(ctx, identity)
	if err != nil {
		return err
	}

	ew.log.Infow("Writing SIP message to persistent endpoint",
		"identity", identity,
		"natAddr", natAddr,
		"method", msgMethod(msg),
	)

	if err := conn.WriteMsg(msg); err != nil {
		return fmt.Errorf("write to %s failed: %w", natAddr, err)
	}

	return nil
}

// WriteMsgToAddr sends a SIP message to a specific NAT address via TLS pool.
// Use when you already have the NAT address and don't need Redis lookup.
func (ew *EndpointWriter) WriteMsgToAddr(natAddr string, msg sip.Message) error {
	ew.mu.RLock()
	srv := ew.srv
	ew.mu.RUnlock()

	if srv == nil {
		return fmt.Errorf("sipgo server not available")
	}

	tl := srv.TransportLayer()
	if tl == nil {
		return fmt.Errorf("transport layer not available")
	}

	conn, err := tl.GetConnection("tls", natAddr)
	if err != nil {
		return fmt.Errorf("no TLS connection for %s: %w", natAddr, err)
	}
	if conn == nil {
		return fmt.Errorf("TLS connection not found for %s", natAddr)
	}

	return conn.WriteMsg(msg)
}

func msgMethod(msg sip.Message) string {
	if req, ok := msg.(*sip.Request); ok {
		return string(req.Method)
	}
	if resp, ok := msg.(*sip.Response); ok {
		return fmt.Sprintf("%d", resp.StatusCode)
	}
	return "unknown"
}

// InviteEndpoint builds and sends a SIP INVITE to a registered phone via its
// persistent TLS connection. Returns the Call-ID for response matching.
// The phone will respond with 100/180/200 on the same TLS socket.
func (ew *EndpointWriter) InviteEndpoint(ctx context.Context, identity string, conf *config.SONIQConfig, callerName, callerNumber string, autoAnswer ...bool) (string, error) {
	conn, natAddr, err := ew.GetConnection(ctx, identity)
	if err != nil {
		return "", err
	}

	callID := fmt.Sprintf("soniq-ep-%d@%s", time.Now().UnixNano(), conf.ExternalIP)
	fromTag := fmt.Sprintf("soniq-%d", time.Now().UnixMilli())

	// Build INVITE
	reqURI := sip.Uri{User: identity, Host: conf.Realm, Port: conf.RegPortListen}
	invite := sip.NewRequest(sip.INVITE, reqURI)

	// Via
	via := &sip.ViaHeader{
		ProtocolName:    "SIP",
		ProtocolVersion: "2.0",
		Transport:       "TLS",
		Host:            conf.ExternalIP,
		Port:            conf.RegPortListen,
		Params:          sip.NewParams(),
	}
	via.Params.Add("branch", sip.GenerateBranch())
	via.Params.Add("rport", "")
	invite.AppendHeader(via)

	// From (caller)
	fromDisplay := callerName
	if fromDisplay == "" {
		fromDisplay = callerNumber
	}
	invite.AppendHeader(sip.NewHeader("From",
		fmt.Sprintf("\"%s\" <sip:%s@%s>;tag=%s", fromDisplay, callerNumber, conf.Realm, fromTag)))

	// To (phone)
	invite.AppendHeader(sip.NewHeader("To",
		fmt.Sprintf("<sip:%s@%s:%d>", identity, conf.Realm, conf.RegPortListen)))

	invite.AppendHeader(sip.NewHeader("Call-ID", callID))
	invite.AppendHeader(sip.NewHeader("CSeq", "1 INVITE"))
	invite.AppendHeader(sip.NewHeader("Max-Forwards", "70"))
	invite.AppendHeader(sip.NewHeader("Contact",
		fmt.Sprintf("<sip:soniq@%s:%d;transport=TLS>", conf.ExternalIP, conf.RegPortListen)))
	invite.AppendHeader(sip.NewHeader("Allow", "INVITE, ACK, CANCEL, BYE, NOTIFY, OPTIONS"))

	// Auto-answer: phone picks up immediately without ringing
	if len(autoAnswer) > 0 && autoAnswer[0] {
		invite.AppendHeader(sip.NewHeader("Call-Info", ";answer-after=0"))
		invite.AppendHeader(sip.NewHeader("Alert-Info", "info=alert-autoanswer"))
	}

	invite.AppendHeader(sip.NewHeader("Content-Type", "application/sdp"))

	// SDP — offer G722 + PCMU on server's public IP
	// Use a dynamic RTP port (we'll need to allocate one properly later)
	rtpPort := 20000 + (time.Now().UnixMilli() % 10000)
	sdp := fmt.Sprintf(`v=0
o=soniq %d %d IN IP4 %s
s=SONIQ Call
c=IN IP4 %s
t=0 0
m=audio %d RTP/AVP 9 0 8 101
a=rtpmap:9 G722/8000
a=rtpmap:0 PCMU/8000
a=rtpmap:8 PCMA/8000
a=rtpmap:101 telephone-event/8000
a=fmtp:101 0-16
a=sendrecv
a=ptime:20
`, time.Now().Unix(), time.Now().Unix(), conf.ExternalIP,
		conf.ExternalIP, rtpPort)

	invite.SetBody([]byte(sdp))

	ew.log.Infow("Sending INVITE to registered endpoint",
		"identity", identity,
		"natAddr", natAddr,
		"callID", callID,
		"caller", callerName,
		"callerNum", callerNumber,
		"rtpPort", rtpPort,
	)

	if err := conn.WriteMsg(invite); err != nil {
		return "", fmt.Errorf("INVITE write to %s failed: %w", natAddr, err)
	}

	// Track this pending call for response matching
	pc := &PendingCall{
		CallID:       callID,
		Identity:     identity,
		NatAddr:      natAddr,
		FromTag:      fromTag,
		RTPPort:      int(rtpPort),
		State:        "invited",
		Conf:         conf,
		AnsweredChan: make(chan struct{}),
	}
	ew.pendingMu.Lock()
	ew.pending[callID] = pc
	ew.pendingMu.Unlock()

	return callID, nil
}

// HandleResponse processes a SIP response that arrived on the TLS connection.
// Called from OnNoRoute or a response interceptor when sipgo can't match
// a response to an existing transaction.
func (ew *EndpointWriter) HandleResponse(resp *sip.Response) bool {
	callID := ""
	if h := resp.CallID(); h != nil {
		callID = h.Value()
	}
	if callID == "" {
		return false
	}

	ew.pendingMu.Lock()
	pc, ok := ew.pending[callID]
	ew.pendingMu.Unlock()
	if !ok {
		return false // not our call
	}

	// Extract To tag from response
	if h := resp.To(); h != nil {
		if tag, ok := h.Params.Get("tag"); ok {
			pc.ToTag = tag
		}
	}

	switch {
	case resp.StatusCode >= 100 && resp.StatusCode < 200:
		// Provisional (100 Trying, 180 Ringing, 183 Session Progress)
		pc.State = "ringing"
		ew.log.Infow("Phone ringing",
			"identity", pc.Identity,
			"callID", callID,
			"status", resp.StatusCode,
			"toTag", pc.ToTag,
		)
		return true

	case resp.StatusCode == 200:
		// 200 OK — phone answered! Send ACK and extract SDP answer
		pc.State = "answered"
		pc.RemoteSDP = string(resp.Body())
		ew.log.Infow("Phone answered!",
			"identity", pc.Identity,
			"callID", callID,
			"toTag", pc.ToTag,
			"sdpLen", len(pc.RemoteSDP),
		)

		// Send ACK via persistent TLS
		go ew.sendACK(pc, resp)

		// Signal answered
		select {
		case <-pc.AnsweredChan:
		default:
			close(pc.AnsweredChan)
		}
		return true

	case resp.StatusCode >= 300:
		// Error response — call failed
		ew.log.Warnw("INVITE rejected", fmt.Errorf("status %d", resp.StatusCode),
			"identity", pc.Identity,
			"callID", callID,
			"status", resp.StatusCode,
			"reason", resp.Reason,
		)
		pc.State = "failed"
		ew.pendingMu.Lock()
		delete(ew.pending, callID)
		ew.pendingMu.Unlock()
		return true
	}

	return false
}

// sendACK sends an ACK for a 200 OK response via the persistent TLS connection.
func (ew *EndpointWriter) sendACK(pc *PendingCall, resp *sip.Response) {
	reqURI := sip.Uri{User: pc.Identity, Host: pc.Conf.Realm, Port: pc.Conf.RegPortListen}
	ack := sip.NewRequest(sip.ACK, reqURI)

	// Via
	via := &sip.ViaHeader{
		ProtocolName:    "SIP",
		ProtocolVersion: "2.0",
		Transport:       "TLS",
		Host:            pc.Conf.ExternalIP,
		Port:            pc.Conf.RegPortListen,
		Params:          sip.NewParams(),
	}
	via.Params.Add("branch", sip.GenerateBranch())
	ack.AppendHeader(via)

	// From must match INVITE's From (with our tag)
	ack.AppendHeader(sip.NewHeader("From",
		fmt.Sprintf("<sip:soniq@%s>;tag=%s", pc.Conf.Realm, pc.FromTag)))
	// To must include phone's tag from 200 OK
	ack.AppendHeader(sip.NewHeader("To",
		fmt.Sprintf("<sip:%s@%s:%d>;tag=%s", pc.Identity, pc.Conf.Realm, pc.Conf.RegPortListen, pc.ToTag)))
	ack.AppendHeader(sip.NewHeader("Call-ID", pc.CallID))
	ack.AppendHeader(sip.NewHeader("CSeq", "1 ACK"))
	ack.AppendHeader(sip.NewHeader("Max-Forwards", "70"))

	ctx := context.Background()
	if err := ew.WriteMsg(ctx, pc.Identity, ack); err != nil {
		ew.log.Errorw("ACK send failed", err, "callID", pc.CallID)
		return
	}

	ew.log.Infow("ACK sent via persistent TLS",
		"identity", pc.Identity,
		"callID", pc.CallID,
	)
}

// SendBYE ends an active call via persistent TLS.
func (ew *EndpointWriter) SendBYE(ctx context.Context, callID string) error {
	ew.pendingMu.Lock()
	pc, ok := ew.pending[callID]
	if ok {
		delete(ew.pending, callID)
	}
	ew.pendingMu.Unlock()
	if !ok {
		return fmt.Errorf("no pending call with ID %s", callID)
	}

	reqURI := sip.Uri{User: pc.Identity, Host: pc.Conf.Realm, Port: pc.Conf.RegPortListen}
	bye := sip.NewRequest(sip.BYE, reqURI)

	via := &sip.ViaHeader{
		ProtocolName:    "SIP",
		ProtocolVersion: "2.0",
		Transport:       "TLS",
		Host:            pc.Conf.ExternalIP,
		Port:            pc.Conf.RegPortListen,
		Params:          sip.NewParams(),
	}
	via.Params.Add("branch", sip.GenerateBranch())
	bye.AppendHeader(via)
	bye.AppendHeader(sip.NewHeader("From",
		fmt.Sprintf("<sip:soniq@%s>;tag=%s", pc.Conf.Realm, pc.FromTag)))
	bye.AppendHeader(sip.NewHeader("To",
		fmt.Sprintf("<sip:%s@%s:%d>;tag=%s", pc.Identity, pc.Conf.Realm, pc.Conf.RegPortListen, pc.ToTag)))
	bye.AppendHeader(sip.NewHeader("Call-ID", pc.CallID))
	bye.AppendHeader(sip.NewHeader("CSeq", "2 BYE"))
	bye.AppendHeader(sip.NewHeader("Max-Forwards", "70"))

	if err := ew.WriteMsg(ctx, pc.Identity, bye); err != nil {
		return fmt.Errorf("BYE send failed: %w", err)
	}

	ew.log.Infow("BYE sent via persistent TLS",
		"identity", pc.Identity,
		"callID", pc.CallID,
	)
	return nil
}

// GetPendingCall returns a pending call by Call-ID.
func (ew *EndpointWriter) GetPendingCall(callID string) *PendingCall {
	ew.pendingMu.Lock()
	defer ew.pendingMu.Unlock()
	return ew.pending[callID]
}
