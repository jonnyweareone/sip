// SONIQ Action URL Handler — Yealink button presses → soniq-router via Ably
//
// Thin HTTP server that receives Yealink action URL POSTs, authenticates the
// device via Redis registration lookup, and publishes structured events to
// soniq-router through Ably. Same event format as WebRTC action chips.

package sip

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/rpc"
	"github.com/livekit/sipgo/sip"
	goredis "github.com/redis/go-redis/v9"

	"github.com/livekit/sip/pkg/config"
)

// ActionEvent is the structured event published to soniq-router via Ably.
type ActionEvent struct {
	Type      string            `json:"type"`       // "button_press", "execute_chip", "dss_key"
	Action    string            `json:"action"`     // "mute", "hold", "transfer", "dnd", "park", "blf", "dial", "chip_execute"
	Identity  string            `json:"identity"`   // LiveKit identity e.g. "1002.soniq-master"
	OrgID     string            `json:"org_id"`     // from registration
	DeviceMAC string            `json:"device_mac"` // from registration
	NodeID    string            `json:"node_id"`    // which SIP node
	Params    map[string]string `json:"params"`     // action-specific: target, chip_id, etc.
	Timestamp int64             `json:"timestamp"`
}

// ActionServer handles HTTP requests from Yealink action URLs.
type ActionServer struct {
	log       logger.Logger
	conf      *config.SONIQConfig
	redis     goredis.UniversalClient
	mux       *http.ServeMux
	srv       *http.Server
	sipCli    *Client // reference to SIP client for CreateSIPParticipant
	registrar *Registrar // for endpoint resolution
	epWriter  *EndpointWriter // persistent TLS connection writer
}

func NewActionServer(conf *config.SONIQConfig, log logger.Logger, rc goredis.UniversalClient) *ActionServer {
	a := &ActionServer{
		log:   log.WithValues("component", "soniq-actions"),
		conf:  conf,
		redis: rc,
		mux:   http.NewServeMux(),
	}
	a.registerRoutes()
	return a
}

// SetSIPClient sets the SIP client reference after construction (avoids circular deps)
func (a *ActionServer) SetSIPClient(cli *Client) {
	a.sipCli = cli
}

func (a *ActionServer) registerRoutes() {
	// POST /actions/{identity}/{action}
	// POST /actions/{identity}/execute-chip/{chip_id}
	// POST /actions/{identity}/dial/{target}
	// POST /actions/{identity}/xml-response/{prompt_id}/{key}
	a.mux.HandleFunc("/actions/", a.handleAction)

	// POST /api/invite-to-room — soniq-router calls this to ring a registered SIP device
	// Body: { "extension": "1000", "org_slug": "soniq-master", "room_name": "...",
	//         "caller_number": "+447...", "caller_name": "Jonny", "participant_identity": "sip-1000" }
	// This invites the phone directly via the existing TLS connection — no trunk needed.
	a.mux.HandleFunc("/api/invite-to-room", a.handleInviteToRoom)

	// POST /api/page — send XML screen pop to a registered phone via persistent TLS
	a.mux.HandleFunc("/api/page", a.handlePage)

	// Health check
	a.mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	})
}

func (a *ActionServer) Start() error {
	addr := fmt.Sprintf(":%d", a.conf.ActionPort)
	a.srv = &http.Server{
		Addr:         addr,
		Handler:      a.mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	a.log.Infow("action URL server starting", "addr", addr)
	go func() {
		if err := a.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			a.log.Errorw("action server error", err)
		}
	}()
	return nil
}

func (a *ActionServer) Stop() {
	if a.srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = a.srv.Shutdown(ctx)
	}
}

// handleAction processes all action URL requests.
// URL pattern: /actions/{identity}/{action}[/{extra}]
func (a *ActionServer) handleAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		http.Error(w, "method not allowed", 405)
		return
	}

	// Parse path: /actions/{identity}/{action}[/{extra...}]
	path := strings.TrimPrefix(r.URL.Path, "/actions/")
	parts := strings.SplitN(path, "/", 3)
	if len(parts) < 2 {
		http.Error(w, "invalid action path", 400)
		return
	}

	identity := parts[0]
	action := parts[1]
	extra := ""
	if len(parts) > 2 {
		extra = parts[2]
	}

	// Authenticate: verify this identity is registered on this node
	ctx := r.Context()
	key := redisEndpointPrefix + identity
	fields, err := a.redis.HGetAll(ctx, key).Result()
	if err != nil || len(fields) == 0 {
		a.log.Warnw("action from unregistered device", nil, "identity", identity)
		http.Error(w, "device not registered", 403)
		return
	}

	// Build params from query string + extra path segment
	params := make(map[string]string)
	for k, v := range r.URL.Query() {
		if len(v) > 0 {
			params[k] = v[0]
		}
	}
	if extra != "" {
		// For execute-chip/{chip_id} or dial/{target} or xml-response/{prompt_id}/{key}
		switch action {
		case "execute-chip":
			params["chip_id"] = extra
		case "dial":
			params["target"] = extra
		case "xml-response":
			// extra = "{prompt_id}/{key}"
			rParts := strings.SplitN(extra, "/", 2)
			if len(rParts) == 2 {
				params["prompt_id"] = rParts[0]
				params["key"] = rParts[1]
			}
		default:
			params["extra"] = extra
		}
	}

	event := ActionEvent{
		Type:      "button_press",
		Action:    action,
		Identity:  identity,
		OrgID:     fields["org_id"],
		DeviceMAC: fields["mac"],
		NodeID:    a.conf.NodeID,
		Params:    params,
		Timestamp: time.Now().UnixMilli(),
	}

	a.log.Infow("action received",
		"identity", identity,
		"action", action,
		"params", params,
	)

	// Publish to Ably for soniq-router
	if err := a.publishToAbly(ctx, event); err != nil {
		a.log.Errorw("failed to publish action event", err)
		http.Error(w, "publish failed", 500)
		return
	}

	// Return Yealink XML response (acknowledge)
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(200)
	_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><YealinkIPPhoneText><Title>OK</Title><Text>Action received</Text></YealinkIPPhoneText>`))
}

// publishToAbly sends the action event to Ably REST API for soniq-router.
// Channel: soniq:actions:{org_id}
func (a *ActionServer) publishToAbly(ctx context.Context, event ActionEvent) error {
	if a.conf.AblyAPIKey == "" {
		a.log.Warnw("ably API key not configured, skipping publish", nil)
		return nil
	}

	channel := fmt.Sprintf("soniq:actions:%s", event.OrgID)
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	// Ably REST publish: POST https://rest.ably.io/channels/{channel}/messages
	url := fmt.Sprintf("https://rest.ably.io/channels/%s/messages", channel)
	body := fmt.Sprintf(`{"name":"%s","data":%s}`, event.Action, string(payload))

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.SetBasicAuth(strings.SplitN(a.conf.AblyAPIKey, ":", 2)[0], strings.SplitN(a.conf.AblyAPIKey, ":", 2)[1])

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("ably publish failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("ably returned %d", resp.StatusCode)
	}
	return nil
}

// ── Invite-to-Room API ───────────────────────────────────────────────────
// POST /api/invite-to-room
// Called by soniq-router to ring a registered SIP deskphone into a LiveKit room.
// This replaces the old CreateSIPParticipant → trunk → Drachtio roundabout.
// The phone is invited directly via its existing TLS registration connection.

type inviteToRoomReq struct {
	Extension           string `json:"extension"`             // e.g. "1000"
	OrgSlug             string `json:"org_slug"`              // e.g. "soniq-master"
	RoomName            string `json:"room_name"`             // LiveKit room to join
	CallerNumber        string `json:"caller_number"`         // CLI for display
	CallerName          string `json:"caller_name"`           // Display name
	ParticipantIdentity string `json:"participant_identity"`  // e.g. "sip-1000"
	ParticipantName     string `json:"participant_name"`      // e.g. "Jonny Robinson"
}

func (a *ActionServer) handleInviteToRoom(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}

	// Auth: check shared secret
	secret := r.Header.Get("X-Internal-Secret")
	if secret != "sNq-nTfY-2026-xK9p" {
		http.Error(w, "forbidden", 403)
		return
	}

	var req inviteToRoomReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), 400)
		return
	}

	if req.Extension == "" || req.RoomName == "" {
		http.Error(w, "extension and room_name required", 400)
		return
	}

	if req.OrgSlug == "" {
		req.OrgSlug = "soniq-master"
	}
	if req.ParticipantIdentity == "" {
		req.ParticipantIdentity = "sip-" + req.Extension
	}

	identity := req.Extension + "." + req.OrgSlug

	// Resolve endpoint from Redis — get NAT address for direct routing
	// This mirrors deskphone.go's handleInternalCall which sets Address + Transport
	endpointContact, endpointFields, _ := a.resolveEndpointForInvite(r.Context(), identity)

	a.log.Infow("invite-to-room",
		"extension", req.Extension,
		"room", req.RoomName,
		"caller", req.CallerNumber,
		"identity", identity,
		"contact", endpointContact,
	)

	if a.sipCli == nil {
		http.Error(w, "SIP client not available", 503)
		return
	}

	// Build address from endpoint fields (same as deskphone.go)
	address := ""
	if endpointFields != nil {
		address = endpointFields["nat_ip"] + ":" + endpointFields["nat_port"]
	}

	ctx := r.Context()
	callerName := req.CallerName
	resp, err := a.sipCli.CreateSIPParticipant(ctx, &rpc.InternalCreateSIPParticipantRequest{
		SipCallId:           fmt.Sprintf("soniq-invite-%d", time.Now().UnixMilli()),
		Address:             address,
		Transport:           3, // SIP_TRANSPORT_TLS = 3
		CallTo:              identity,
		Number:              req.CallerNumber,
		DisplayName:         &callerName,
		RoomName:            req.RoomName,
		ParticipantIdentity: req.ParticipantIdentity,
		ParticipantName:     req.ParticipantName,
		WaitUntilAnswered:   true,
	})

	if err != nil {
		a.log.Errorw("invite-to-room failed", err, "identity", identity)
		http.Error(w, "invite failed: "+err.Error(), 500)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":             true,
		"participant_id": resp.GetParticipantId(),
		"identity":       resp.GetParticipantIdentity(),
	})
}

// resolveEndpointForInvite looks up a registered endpoint for invite-to-room.
func (a *ActionServer) resolveEndpointForInvite(ctx context.Context, identity string) (string, map[string]string, error) {
	if a.registrar != nil {
		return a.registrar.ResolveEndpoint(ctx, identity)
	}
	// Fallback to Redis direct lookup
	key := redisEndpointPrefix + identity
	result, err := a.redis.HGetAll(ctx, key).Result()
	if err != nil || len(result) == 0 {
		return "", nil, fmt.Errorf("endpoint not found: %s", identity)
	}
	return result["contact_uri"], result, nil
}

// handlePage sends Yealink XML via SIP NOTIFY through the persistent TLS connection.
// POST /api/page
// Body: { "extension": "1000", "org_slug": "soniq-master", "type": "text|execute|config",
//         "title": "...", "text": "...", "beep": true, "timeout": 10,
//         "items": ["Led:LINE2_GREEN=on"] }
func (a *ActionServer) handlePage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}

	var req struct {
		Extension string   `json:"extension"`
		OrgSlug   string   `json:"org_slug"`
		Type      string   `json:"type"`     // "text", "execute", "config"
		Title     string   `json:"title"`
		Text      string   `json:"text"`
		Beep      bool     `json:"beep"`
		Timeout   int      `json:"timeout"`
		Items     []string `json:"items"`    // for execute/config
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), 400)
		return
	}
	if req.Extension == "" {
		http.Error(w, "extension required", 400)
		return
	}
	if req.OrgSlug == "" {
		req.OrgSlug = "soniq-master"
	}
	if req.Type == "" {
		req.Type = "text"
	}
	if req.Timeout == 0 {
		req.Timeout = 10
	}

	identity := req.Extension + "." + req.OrgSlug

	// Build Yealink XML body based on type
	var xmlBody string
	beepStr := "no"
	if req.Beep {
		beepStr = "yes"
	}

	switch req.Type {
	case "text":
		xmlBody = fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<YealinkIPPhoneTextScreen Beep="%s" Timeout="%d">
<Title>%s</Title>
<Text>%s</Text>
</YealinkIPPhoneTextScreen>`, beepStr, req.Timeout, req.Title, req.Text)

	case "execute":
		items := ""
		for _, item := range req.Items {
			items += fmt.Sprintf(`<ExecuteItem URI="%s"/>`, item)
		}
		xmlBody = fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<YealinkIPPhoneExecute Beep="%s">
%s
</YealinkIPPhoneExecute>`, beepStr, items)

	case "config":
		items := ""
		for _, item := range req.Items {
			items += fmt.Sprintf(`<Item>%s</Item>`, item)
		}
		xmlBody = fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<YealinkIPPhoneConfiguration>
%s
</YealinkIPPhoneConfiguration>`, items)

	default:
		http.Error(w, "type must be text, execute, or config", 400)
		return
	}

	if a.epWriter == nil {
		http.Error(w, "endpoint writer not available", 503)
		return
	}

	// Build SIP NOTIFY with XML body
	reqURI := sip.Uri{User: identity, Host: a.conf.Realm}
	notify := sip.NewRequest(sip.NOTIFY, reqURI)

	// Via header required
	via := &sip.ViaHeader{
		ProtocolName:    "SIP",
		ProtocolVersion: "2.0",
		Transport:       "TLS",
		Host:            a.conf.ExternalIP,
		Port:            a.conf.RegPortListen,
		Params:          sip.NewParams(),
	}
	via.Params.Add("branch", sip.GenerateBranch())
	notify.AppendHeader(via)
	notify.AppendHeader(sip.NewHeader("From", fmt.Sprintf("<sip:soniq@%s>;tag=page-%d", a.conf.Realm, time.Now().UnixMilli())))
	notify.AppendHeader(sip.NewHeader("To", fmt.Sprintf("<sip:%s@%s>", identity, a.conf.Realm)))
	notify.AppendHeader(sip.NewHeader("Call-ID", fmt.Sprintf("page-%d@%s", time.Now().UnixNano(), a.conf.ExternalIP)))
	notify.AppendHeader(sip.NewHeader("CSeq", "1 NOTIFY"))
	notify.AppendHeader(sip.NewHeader("Event", "xml-push"))
	notify.AppendHeader(sip.NewHeader("Subscription-State", "active"))
	notify.AppendHeader(sip.NewHeader("Content-Type", "application/xml"))
	notify.AppendHeader(sip.NewHeader("Max-Forwards", "70"))
	notify.SetBody([]byte(xmlBody))

	ctx := r.Context()
	if err := a.epWriter.WriteMsg(ctx, identity, notify); err != nil {
		a.log.Errorw("page failed", err, "identity", identity, "type", req.Type)
		http.Error(w, "page failed: "+err.Error(), 500)
		return
	}

	a.log.Infow("page sent", "identity", identity, "type", req.Type, "title", req.Title)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "identity": identity, "type": req.Type})
}
