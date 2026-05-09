// SONIQ BLF — Busy Lamp Field via SIP SUBSCRIBE/NOTIFY + XML context screens
//
// Handles SIP SUBSCRIBE (Event: dialog) from Yealink deskphones.
// Stores subscriptions in Redis. Pushes NOTIFY on presence change.
// Provides XML context screen when BLF key is pressed (action URL).
//
// Flow:
//   Phone SUBSCRIBE ext 1000 → 200 OK + initial NOTIFY (current state)
//   Presence changes (Ably event) → fan out NOTIFY to all subscribers
//   Phone presses BLF key → HTTP GET action URL → XML context screen
//
// Redis keys:
//   sip:blf:sub:{subscriber}:{target}  — subscription state (hash)
//   sip:blf:subs-for:{target}          — set of subscriber identities
//   sip:presence:{identity}            — current presence state (hash)

package sip

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/livekit/protocol/logger"
	"github.com/livekit/sipgo/sip"
	goredis "github.com/redis/go-redis/v9"

	"github.com/livekit/sip/pkg/config"
)

const (
	redisBLFSubPrefix   = "sip:blf:sub:"    // per-subscription hash
	redisBLFSubsFor     = "sip:blf:subs-for:" // set of subscribers per target
	redisPresencePrefix = "sip:presence:"    // current presence state
	blfDefaultExpiry    = 1800               // 30 min default subscription
	blfMaxExpiry        = 3600               // 1 hour max
)

// PresenceState represents a user's current presence.
type PresenceState struct {
	State     string `json:"state"`      // idle, ringing, confirmed, held, dnd, offline
	Direction string `json:"direction"`  // initiator, recipient, ""
	CallID    string `json:"call_id"`    // active call ID if any
	RemoteURI string `json:"remote_uri"` // who they're talking to
	Context   string `json:"context"`    // "Call with +447..." or "In meeting"
	Since     int64  `json:"since"`      // unix timestamp
	Version   int64  `json:"version"`    // monotonic version for NOTIFY
}

// BLFManager handles BLF subscriptions and NOTIFY fan-out.
type BLFManager struct {
	log       logger.Logger
	conf      *config.SONIQConfig
	redis     goredis.UniversalClient
	registrar *Registrar
	sipCli    SIPClient // sipgo client for sending NOTIFYs
	version   atomic.Int64
}

func NewBLFManager(conf *config.SONIQConfig, log logger.Logger, rc goredis.UniversalClient, reg *Registrar) *BLFManager {
	return &BLFManager{
		log:       log.WithValues("component", "soniq-blf"),
		conf:      conf,
		redis:     rc,
		registrar: reg,
	}
}

// SetSIPClient provides the sipgo client for sending outbound NOTIFYs.
func (b *BLFManager) SetSIPClient(cli SIPClient) {
	b.sipCli = cli
}

// OnSubscribe handles SIP SUBSCRIBE requests (Event: dialog).
func (b *BLFManager) OnSubscribe(log *slog.Logger, req *sip.Request, tx sip.ServerTransaction) {
	ctx := context.Background()

	// Extract headers
	from := req.From()
	to := req.To()
	if from == nil || to == nil {
		_ = tx.Respond(sip.NewResponseFromRequest(req, 400, "Bad Request", nil))
		return
	}

	subscriber := from.Address.User // e.g. "1003.soniq-master"
	target := to.Address.User       // e.g. "1000" (just extension, no org)

	callID := ""
	if h := req.CallID(); h != nil {
		callID = h.Value()
	}

	// Parse Event header — we handle "dialog" (BLF) and "message-summary" (MWI)
	eventHdr := req.GetHeader("Event")
	eventType := "dialog"
	if eventHdr != nil {
		eventType = strings.TrimSpace(strings.SplitN(eventHdr.Value(), ";", 2)[0])
	}

	switch eventType {
	case "dialog", "presence":
		// BLF — handled below
	case "message-summary":
		// MWI — voicemail waiting indicator
		b.handleMWISubscribe(ctx, subscriber, target, callID, req, tx)
		return
	default:
		b.log.Infow("SUBSCRIBE for unsupported event", "event", eventType, "from", subscriber)
		_ = tx.Respond(sip.NewResponseFromRequest(req, 489, "Bad Event", nil))
		return
	}

	// Parse Expires
	expires := blfDefaultExpiry
	if exHdr := req.GetHeader("Expires"); exHdr != nil {
		if v, err := strconv.Atoi(strings.TrimSpace(exHdr.Value())); err == nil {
			if v == 0 {
				// Unsubscribe
				b.removeSubscription(ctx, subscriber, target)
				res := sip.NewResponseFromRequest(req, 200, "OK", nil)
				res.AppendHeader(sip.NewHeader("Expires", "0"))
				_ = tx.Respond(res)
				b.log.Infow("BLF unsubscribed", "subscriber", subscriber, "target", target)
				return
			}
			if v > blfMaxExpiry {
				v = blfMaxExpiry
			}
			expires = v
		}
	}

	// Resolve target to full identity (add org if missing)
	targetIdentity := target
	if !strings.Contains(target, ".") {
		// Bare extension — resolve org from subscriber's registration
		subKey := redisEndpointPrefix + subscriber
		subFields, _ := b.redis.HGetAll(ctx, subKey).Result()
		if orgID := subFields["org_id"]; orgID != "" {
			// Look up org slug from subscriber identity
			parts := strings.SplitN(subscriber, ".", 2)
			if len(parts) == 2 {
				targetIdentity = target + "." + parts[1]
			}
		}
	}

	b.log.Infow("BLF SUBSCRIBE",
		"subscriber", subscriber,
		"target", targetIdentity,
		"expires", expires,
		"event", eventType,
		"callID", callID,
	)

	// Store subscription in Redis
	b.storeSubscription(ctx, subscriber, targetIdentity, callID, expires, req)

	// 200 OK
	res := sip.NewResponseFromRequest(req, 200, "OK", nil)
	res.AppendHeader(sip.NewHeader("Expires", strconv.Itoa(expires)))
	_ = tx.Respond(res)

	// Send initial NOTIFY with current state
	go b.sendNotify(ctx, subscriber, targetIdentity, callID)
}

// storeSubscription saves BLF subscription to Redis with dialog headers for NOTIFY.
func (b *BLFManager) storeSubscription(ctx context.Context, subscriber, target, callID string, expires int, req *sip.Request) {
	subKey := redisBLFSubPrefix + subscriber + ":" + target
	ttl := time.Duration(float64(expires)*1.5) * time.Second

	// Store dialog headers needed for NOTIFY
	fromHdr := ""
	toHdr := ""
	if f := req.From(); f != nil {
		fromHdr = f.Value()
	}
	if t := req.To(); t != nil {
		toHdr = t.Value()
	}

	pipe := b.redis.Pipeline()
	pipe.HSet(ctx, subKey, map[string]interface{}{
		"subscriber": subscriber,
		"target":     target,
		"call_id":    callID,
		"from_hdr":   fromHdr,
		"to_hdr":     toHdr,
		"expires":    strconv.Itoa(expires),
		"created_at": strconv.FormatInt(time.Now().Unix(), 10),
	})
	pipe.Expire(ctx, subKey, ttl)
	subsForKey := redisBLFSubsFor + target
	pipe.SAdd(ctx, subsForKey, subscriber)
	pipe.Expire(ctx, subsForKey, ttl)
	pipe.Exec(ctx)
}

// removeSubscription removes a BLF subscription.
func (b *BLFManager) removeSubscription(ctx context.Context, subscriber, target string) {
	subKey := redisBLFSubPrefix + subscriber + ":" + target
	b.redis.Del(ctx, subKey)
	subsForKey := redisBLFSubsFor + target
	b.redis.SRem(ctx, subsForKey, subscriber)
}

// GetPresence gets the current presence state for an identity.
func (b *BLFManager) GetPresence(ctx context.Context, identity string) (*PresenceState, error) {
	key := redisPresencePrefix + identity
	result, err := b.redis.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return &PresenceState{State: "idle", Since: time.Now().Unix()}, nil
	}
	since, _ := strconv.ParseInt(result["since"], 10, 64)
	version, _ := strconv.ParseInt(result["version"], 10, 64)
	return &PresenceState{
		State:     result["state"],
		Direction: result["direction"],
		CallID:    result["call_id"],
		RemoteURI: result["remote_uri"],
		Context:   result["context"],
		Since:     since,
		Version:   version,
	}, nil
}

// SetPresence updates presence and fans out NOTIFYs to all subscribers.
func (b *BLFManager) SetPresence(ctx context.Context, identity string, state PresenceState) error {
	key := redisPresencePrefix + identity
	state.Since = time.Now().Unix()
	state.Version = b.version.Add(1)

	pipe := b.redis.Pipeline()
	pipe.HSet(ctx, key, map[string]interface{}{
		"state":      state.State,
		"direction":  state.Direction,
		"call_id":    state.CallID,
		"remote_uri": state.RemoteURI,
		"context":    state.Context,
		"since":      strconv.FormatInt(state.Since, 10),
		"version":    strconv.FormatInt(state.Version, 10),
	})
	pipe.Expire(ctx, key, 24*time.Hour)
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}

	b.log.Infow("Presence updated",
		"identity", identity,
		"state", state.State,
		"context", state.Context,
	)

	// Fan out NOTIFYs to all subscribers of this identity
	go b.fanOutNotify(ctx, identity)
	return nil
}

// fanOutNotify sends NOTIFY to all phones subscribed to this target.
func (b *BLFManager) fanOutNotify(ctx context.Context, target string) {
	subsForKey := redisBLFSubsFor + target
	subscribers, err := b.redis.SMembers(ctx, subsForKey).Result()
	if err != nil {
		b.log.Errorw("failed to get subscribers", err, "target", target)
		return
	}

	for _, subscriber := range subscribers {
		// Get the subscription's callID for the NOTIFY
		subKey := redisBLFSubPrefix + subscriber + ":" + target
		fields, err := b.redis.HGetAll(ctx, subKey).Result()
		if err != nil || len(fields) == 0 {
			continue
		}
		callID := fields["call_id"]
		b.sendNotify(ctx, subscriber, target, callID)
	}
}

// sendNotify sends a SIP NOTIFY with dialog-info XML to a subscriber.
func (b *BLFManager) sendNotify(ctx context.Context, subscriber, target, callID string) {
	if b.sipCli == nil {
		b.log.Debugw("No SIP client — cannot send NOTIFY", "subscriber", subscriber)
		return
	}

	presence, err := b.GetPresence(ctx, target)
	if err != nil {
		b.log.Errorw("failed to get presence for NOTIFY", err, "target", target)
		return
	}

	// Look up subscriber's contact URI from Redis registration
	contactURI, subFields, err := b.registrar.ResolveEndpoint(ctx, subscriber)
	if err != nil || subFields == nil || contactURI == "" {
		b.log.Debugw("subscriber not registered, skipping NOTIFY", "subscriber", subscriber)
		return
	}

	// Get subscription dialog headers
	subKey := redisBLFSubPrefix + subscriber + ":" + target
	subData, _ := b.redis.HGetAll(ctx, subKey).Result()

	// Build dialog-info XML body
	xml := b.buildDialogInfoXML(target, presence)

	// Parse the subscriber's contact URI to build the NOTIFY request-URI
	// The contact is like: <sip:1000.soniq-master@192.168.0.170:50030;transport=TLS>
	// We must send to this address so sipgo uses the existing TLS connection
	parsedContact := contactURI
	parsedContact = strings.TrimPrefix(parsedContact, "<")
	parsedContact = strings.TrimSuffix(parsedContact, ">")
	// Strip params after semicolon for the URI
	contactAddr := parsedContact
	if idx := strings.Index(contactAddr, ";"); idx > 0 {
		contactAddr = contactAddr[:idx]
	}
	// Parse sip:user@host:port
	contactAddr = strings.TrimPrefix(contactAddr, "sip:")
	contactAddr = strings.TrimPrefix(contactAddr, "sips:")
	contactParts := strings.SplitN(contactAddr, "@", 2)
	contactHost := ""
	contactPort := 0
	if len(contactParts) == 2 {
		hostPort := contactParts[1]
		if hp := strings.SplitN(hostPort, ":", 2); len(hp) == 2 {
			contactHost = hp[0]
			contactPort, _ = strconv.Atoi(hp[1])
		} else {
			contactHost = hostPort
			contactPort = 5060
		}
	}

	// But the phone is behind NAT — use the NAT IP from registration instead
	natIP := subFields["nat_ip"]
	natPort, _ := strconv.Atoi(subFields["nat_port"])
	if natIP != "" && natPort > 0 {
		contactHost = natIP
		contactPort = natPort
	}

	// Build NOTIFY request targeting the phone's actual address
	reqURI := sip.Uri{
		User: subscriber,
		Host: contactHost,
		Port: contactPort,
	}

	req := sip.NewRequest(sip.NOTIFY, reqURI)
	req.AppendHeader(sip.NewHeader("From", fmt.Sprintf("<sip:%s@%s>;tag=blf-%s", target, b.conf.Realm, target)))
	req.AppendHeader(sip.NewHeader("To", fmt.Sprintf("<sip:%s@%s>", subscriber, b.conf.Realm)))
	req.AppendHeader(sip.NewHeader("Call-ID", subData["call_id"]))
	req.AppendHeader(sip.NewHeader("CSeq", "1 NOTIFY"))
	req.AppendHeader(sip.NewHeader("Event", "dialog"))
	req.AppendHeader(sip.NewHeader("Subscription-State", "active;expires=1800"))
	req.AppendHeader(sip.NewHeader("Content-Type", "application/dialog-info+xml"))
	req.AppendHeader(sip.NewHeader("Max-Forwards", "70"))
	req.AppendHeader(sip.NewHeader("Contact", fmt.Sprintf("<sip:%s@%s:%d;transport=tls>",
		target, b.conf.ExternalIP, b.conf.RegPortListen)))
	req.SetBody([]byte(xml))

	b.log.Infow("Sending BLF NOTIFY via SIP",
		"subscriber", subscriber,
		"target", target,
		"state", presence.State,
		"contactURI", contactURI,
	)

	if err := b.sipCli.WriteRequest(req); err != nil {
		b.log.Warnw("Failed to send BLF NOTIFY", err, "subscriber", subscriber, "target", target)
	}
}

// buildDialogInfoXML creates RFC 4235 dialog-info XML for Yealink BLF.
func (b *BLFManager) buildDialogInfoXML(target string, state *PresenceState) string {
	entity := fmt.Sprintf("sip:%s@%s", target, b.conf.Realm)
	version := strconv.FormatInt(state.Version, 10)

	switch state.State {
	case "ringing":
		return fmt.Sprintf(`<?xml version="1.0"?>
<dialog-info xmlns="urn:ietf:params:xml:ns:dialog-info" version="%s" state="full" entity="%s">
  <dialog id="%s" direction="%s">
    <state>early</state>
    <remote><identity>%s</identity></remote>
  </dialog>
</dialog-info>`, version, entity, state.CallID, nvl(state.Direction, "recipient"), state.RemoteURI)

	case "confirmed", "busy":
		return fmt.Sprintf(`<?xml version="1.0"?>
<dialog-info xmlns="urn:ietf:params:xml:ns:dialog-info" version="%s" state="full" entity="%s">
  <dialog id="%s" direction="%s">
    <state>confirmed</state>
    <remote><identity>%s</identity></remote>
  </dialog>
</dialog-info>`, version, entity, state.CallID, nvl(state.Direction, "initiator"), state.RemoteURI)

	case "held":
		return fmt.Sprintf(`<?xml version="1.0"?>
<dialog-info xmlns="urn:ietf:params:xml:ns:dialog-info" version="%s" state="full" entity="%s">
  <dialog id="%s" direction="%s">
    <state>confirmed</state>
    <local><target uri="%s"><param pname="+sip.rendering" pvalue="no"/></target></local>
  </dialog>
</dialog-info>`, version, entity, state.CallID, nvl(state.Direction, "initiator"), entity)

	default: // idle, offline, dnd
		return fmt.Sprintf(`<?xml version="1.0"?>
<dialog-info xmlns="urn:ietf:params:xml:ns:dialog-info" version="%s" state="full" entity="%s">
</dialog-info>`, version, entity)
	}
}

func nvl(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// ── HTTP API for presence updates (called by soniq-router) ───────────────

// RegisterBLFRoutes adds BLF HTTP endpoints to the action server mux.
func (b *BLFManager) RegisterBLFRoutes(mux *http.ServeMux) {
	// POST /api/blf/presence — update user presence (from soniq-router)
	mux.HandleFunc("/api/blf/presence", b.handlePresenceUpdate)

	// GET /api/blf/context/{target_ext} — XML context screen for phone display
	mux.HandleFunc("/api/blf/context/", b.handleContextScreen)

	// POST /api/blf/callback — request callback (from phone softkey)
	mux.HandleFunc("/api/blf/callback", b.handleCallbackRequest)
}

// handlePresenceUpdate receives presence changes from soniq-router.
// POST /api/blf/presence
// Body: {"identity": "1000.soniq-master", "state": "confirmed", "context": "Call with +447...", ...}
func (b *BLFManager) handlePresenceUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}

	secret := r.Header.Get("X-Internal-Secret")
	if secret != "sNq-nTfY-2026-xK9p" {
		http.Error(w, "forbidden", 403)
		return
	}

	var state PresenceState
	var req struct {
		Identity string        `json:"identity"`
		PresenceState
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", 400)
		return
	}

	if req.Identity == "" {
		http.Error(w, "identity required", 400)
		return
	}

	if err := b.SetPresence(r.Context(), req.Identity, req.PresenceState); err != nil {
		http.Error(w, "failed: "+err.Error(), 500)
		return
	}

	_ = state // suppress unused warning
	w.WriteHeader(200)
	_, _ = w.Write([]byte(`{"ok":true}`))
}

// handleContextScreen returns a Yealink XML screen with rich user context.
// GET /api/blf/context/{ext}.{org_slug}?from={subscriber_identity}
//
// If target is idle → returns Dial command (phone just calls directly)
// If target is busy/ringing/dnd → shows context screen with softkeys
func (b *BLFManager) handleContextScreen(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/blf/context/")
	target := strings.TrimSuffix(path, "/")
	if target == "" {
		http.Error(w, "target required", 400)
		return
	}

	fromIdentity := r.URL.Query().Get("from")
	ctx := r.Context()

	// Get current presence
	presence, _ := b.GetPresence(ctx, target)
	if presence == nil {
		presence = &PresenceState{State: "idle"}
	}

	ext := strings.SplitN(target, ".", 2)[0]

	// If idle → just dial directly, no XML screen
	if presence.State == "" || presence.State == "idle" || presence.State == "offline" {
		xml := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<YealinkIPPhoneExecute>
  <ExecuteItem URI="Dial:%s"/>
</YealinkIPPhoneExecute>`, ext)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(xml))
		return
	}

	// Busy/ringing/dnd/held → show context screen
	displayName := b.resolveDisplayName(ctx, target)
	statusLine := b.formatStatusLine(presence)
	contextLine := presence.Context
	if contextLine == "" {
		contextLine = b.formatContextLine(presence)
	}

	actionBase := fmt.Sprintf("http://%s:%d", b.conf.ExternalIP, b.conf.ActionPort)

	xml := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<YealinkIPPhoneTextMenu style="normed">
  <Title>%s</Title>
  <MenuItem>
    <Prompt>%s</Prompt>
    <URI></URI>
  </MenuItem>
  <MenuItem>
    <Prompt>%s</Prompt>
    <URI></URI>
  </MenuItem>
  <SoftKey index="1">
    <Label>Call</Label>
    <URI>Dial:%s</URI>
  </SoftKey>
  <SoftKey index="4">
    <Label>Request</Label>
    <URI>%s/api/blf/callback?from=%s&amp;to=%s</URI>
  </SoftKey>
</YealinkIPPhoneTextMenu>`,
		displayName,
		statusLine,
		contextLine,
		ext,
		actionBase, fromIdentity, target,
	)

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(200)
	_, _ = w.Write([]byte(xml))
}

// handleCallbackRequest creates a callback request.
// POST /api/blf/callback?from=1003.soniq-master&to=1000.soniq-master
//
// Stores the request in Redis. Lilly can intercept on voice calls and
// notify the target user after their current call ends.
func (b *BLFManager) handleCallbackRequest(w http.ResponseWriter, r *http.Request) {
	from := r.URL.Query().Get("from")
	to := r.URL.Query().Get("to")

	if from == "" || to == "" {
		http.Error(w, "from and to required", 400)
		return
	}

	ctx := r.Context()

	// Store callback request in Redis list
	callbackKey := "sip:callbacks:" + to
	callback := map[string]string{
		"from":       from,
		"to":         to,
		"from_name":  b.resolveDisplayName(ctx, from),
		"to_name":    b.resolveDisplayName(ctx, to),
		"created_at": strconv.FormatInt(time.Now().Unix(), 10),
	}
	data, _ := json.Marshal(callback)
	b.redis.LPush(ctx, callbackKey, string(data))
	b.redis.Expire(ctx, callbackKey, 4*time.Hour)

	// Publish callback event to Ably for soniq-router / Lilly
	if b.conf.AblyAPIKey != "" {
		fromParts := strings.SplitN(from, ".", 2)
		orgSlug := "soniq-master"
		if len(fromParts) == 2 {
			orgSlug = fromParts[1]
		}
		channel := fmt.Sprintf("soniq:callbacks:%s", orgSlug)
		payload, _ := json.Marshal(callback)
		b.publishAblyEvent(ctx, channel, "callback_request", string(payload))
	}

	fromName := b.resolveDisplayName(ctx, from)
	b.log.Infow("Callback requested", "from", from, "to", to)

	// Return Yealink XML confirmation
	xml := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<YealinkIPPhoneTextScreen>
  <Title>Callback Requested</Title>
  <Text>%s will be notified to call you back when available.</Text>
</YealinkIPPhoneTextScreen>`, b.resolveDisplayName(ctx, to))

	_ = fromName
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(200)
	_, _ = w.Write([]byte(xml))
}

// ── Helpers ──────────────────────────────────────────────────────────────

func (b *BLFManager) resolveDisplayName(ctx context.Context, identity string) string {
	// Try registration data first
	_, fields, _ := b.registrar.ResolveEndpoint(ctx, identity)
	if fields != nil && fields["display_name"] != "" {
		return fields["display_name"]
	}

	// Try sip_credentials via Supabase
	parts := strings.SplitN(identity, ".", 2)
	ext := parts[0]
	url := fmt.Sprintf("%s/rest/v1/sip_credentials?username=eq.%s&select=display_name&limit=1",
		b.conf.SupabaseURL, identity)
	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return ext
	}
	httpReq.Header.Set("apikey", b.conf.SupabaseAnonKey)
	httpReq.Header.Set("Authorization", "Bearer "+b.conf.SupabaseAnonKey)
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return ext
	}
	defer resp.Body.Close()

	var creds []struct {
		DisplayName *string `json:"display_name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&creds); err != nil || len(creds) == 0 || creds[0].DisplayName == nil {
		return ext
	}
	return *creds[0].DisplayName
}

func (b *BLFManager) formatStatusLine(p *PresenceState) string {
	switch p.State {
	case "ringing":
		return "📞 Ringing"
	case "confirmed", "busy":
		elapsed := time.Since(time.Unix(p.Since, 0)).Truncate(time.Second)
		return fmt.Sprintf("🔴 On Call (%s)", elapsed)
	case "held":
		return "⏸️ On Hold"
	case "dnd":
		return "🔕 Do Not Disturb"
	case "offline":
		return "⚫ Offline"
	default:
		return "🟢 Available"
	}
}

func (b *BLFManager) formatContextLine(p *PresenceState) string {
	switch p.State {
	case "ringing":
		if p.RemoteURI != "" {
			return "Incoming from " + p.RemoteURI
		}
		return "Incoming call"
	case "confirmed", "busy":
		if p.RemoteURI != "" {
			return "Call with " + p.RemoteURI
		}
		return "In a call"
	case "held":
		return "Call on hold"
	case "dnd":
		return "Not accepting calls"
	case "offline":
		return "Phone not registered"
	default:
		return "Ready to take calls"
	}
}

func (b *BLFManager) publishAblyEvent(ctx context.Context, channel, name, data string) {
	if b.conf.AblyAPIKey == "" {
		return
	}
	url := fmt.Sprintf("https://rest.ably.io/channels/%s/messages", channel)
	body := fmt.Sprintf(`{"name":"%s","data":%s}`, name, data)
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(body))
	if err != nil {
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")
	parts := strings.SplitN(b.conf.AblyAPIKey, ":", 2)
	if len(parts) == 2 {
		httpReq.SetBasicAuth(parts[0], parts[1])
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return
	}
	resp.Body.Close()
}

// ── MWI (Message Waiting Indicator) ──────────────────────────────────────

const redisMWISubPrefix = "sip:mwi:sub:" // per-subscription
const redisMWISubsFor   = "sip:mwi:subs-for:" // reverse index

// handleMWISubscribe accepts message-summary SUBSCRIBEs and sends voicemail count.
func (b *BLFManager) handleMWISubscribe(ctx context.Context, subscriber, target, callID string, req *sip.Request, tx sip.ServerTransaction) {
	// Parse Expires
	expires := 3600
	if exHdr := req.GetHeader("Expires"); exHdr != nil {
		if v, err := strconv.Atoi(strings.TrimSpace(exHdr.Value())); err == nil {
			if v == 0 {
				// Unsubscribe MWI
				b.redis.Del(ctx, redisMWISubPrefix+subscriber)
				b.redis.SRem(ctx, redisMWISubsFor+target, subscriber)
				res := sip.NewResponseFromRequest(req, 200, "OK", nil)
				res.AppendHeader(sip.NewHeader("Expires", "0"))
				_ = tx.Respond(res)
				return
			}
			expires = v
		}
	}

	// Resolve target identity
	targetIdentity := target
	if !strings.Contains(target, ".") {
		parts := strings.SplitN(subscriber, ".", 2)
		if len(parts) == 2 {
			targetIdentity = target + "." + parts[1]
		}
	}

	b.log.Infow("MWI SUBSCRIBE",
		"subscriber", subscriber,
		"target", targetIdentity,
		"expires", expires,
	)

	// Store subscription
	ttl := time.Duration(float64(expires)*1.5) * time.Second
	pipe := b.redis.Pipeline()
	pipe.HSet(ctx, redisMWISubPrefix+subscriber, map[string]interface{}{
		"subscriber": subscriber,
		"target":     targetIdentity,
		"call_id":    callID,
	})
	pipe.Expire(ctx, redisMWISubPrefix+subscriber, ttl)
	pipe.SAdd(ctx, redisMWISubsFor+targetIdentity, subscriber)
	pipe.Expire(ctx, redisMWISubsFor+targetIdentity, ttl)
	pipe.Exec(ctx)

	// 200 OK
	res := sip.NewResponseFromRequest(req, 200, "OK", nil)
	res.AppendHeader(sip.NewHeader("Expires", strconv.Itoa(expires)))
	_ = tx.Respond(res)

	// Send initial NOTIFY with current voicemail count
	go b.sendMWINotify(ctx, subscriber, targetIdentity)
}

// sendMWINotify sends voicemail count to subscriber.
func (b *BLFManager) sendMWINotify(ctx context.Context, subscriber, target string) {
	// Query voicemail count from Supabase
	newCount, oldCount := b.getVoicemailCount(ctx, target)

	waiting := "no"
	if newCount > 0 {
		waiting = "yes"
	}

	// RFC 3842 message-summary body
	body := fmt.Sprintf("Messages-Waiting: %s\r\nMessage-Account: sip:%s@%s\r\nVoice-Message: %d/%d\r\n",
		waiting, target, b.conf.Realm, newCount, oldCount)

	b.log.Infow("Sending MWI NOTIFY",
		"subscriber", subscriber,
		"target", target,
		"new", newCount,
		"old", oldCount,
	)

	// Store in Redis for the subscriber's next re-SUBSCRIBE to pick up
	pendingKey := "sip:mwi:pending:" + subscriber
	b.redis.Set(ctx, pendingKey, body, 5*time.Minute)
}

// getVoicemailCount queries Supabase for voicemail counts.
func (b *BLFManager) getVoicemailCount(ctx context.Context, identity string) (newCount, oldCount int) {
	// Extract extension from identity (e.g. "1003.soniq-master" → "1003")
	ext := strings.SplitN(identity, ".", 2)[0]

	// Query voicemails table for this user's extension
	// Count new (unread) and old (read) voicemails
	url := fmt.Sprintf("%s/rest/v1/voicemails?select=id,is_read&extension=eq.%s",
		b.conf.SupabaseURL, ext)
	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return 0, 0
	}
	httpReq.Header.Set("apikey", b.conf.SupabaseAnonKey)
	httpReq.Header.Set("Authorization", "Bearer "+b.conf.SupabaseAnonKey)

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		b.log.Debugw("Voicemail query failed", "err", err.Error())
		return 0, 0
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		// Table might not exist yet — return 0/0
		return 0, 0
	}

	var vms []struct {
		ID     string `json:"id"`
		IsRead bool   `json:"is_read"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&vms); err != nil {
		return 0, 0
	}

	for _, vm := range vms {
		if vm.IsRead {
			oldCount++
		} else {
			newCount++
		}
	}
	return newCount, oldCount
}

// UpdateMWI pushes voicemail count update to all subscribed phones for a user.
// Called by soniq-router when a new voicemail is recorded or marked as read.
func (b *BLFManager) UpdateMWI(ctx context.Context, identity string) {
	subsForKey := redisMWISubsFor + identity
	subscribers, err := b.redis.SMembers(ctx, subsForKey).Result()
	if err != nil || len(subscribers) == 0 {
		// The user's own phone subscribes to their own MWI
		// So also check if the identity itself has a subscription
		b.sendMWINotify(ctx, identity, identity)
		return
	}
	for _, sub := range subscribers {
		b.sendMWINotify(ctx, sub, identity)
	}
}

// RegisterMWIRoutes adds MWI HTTP endpoints to the action server mux.
func (b *BLFManager) RegisterMWIRoutes(mux *http.ServeMux) {
	// POST /api/mwi/update — trigger MWI update (from soniq-router after voicemail)
	mux.HandleFunc("/api/mwi/update", b.handleMWIUpdate)
}

// handleMWIUpdate receives MWI update requests from soniq-router.
// POST /api/mwi/update
// Body: {"identity": "1000.soniq-master"}
func (b *BLFManager) handleMWIUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	secret := r.Header.Get("X-Internal-Secret")
	if secret != "sNq-nTfY-2026-xK9p" {
		http.Error(w, "forbidden", 403)
		return
	}

	var req struct {
		Identity string `json:"identity"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Identity == "" {
		http.Error(w, "identity required", 400)
		return
	}

	b.UpdateMWI(r.Context(), req.Identity)
	w.WriteHeader(200)
	_, _ = w.Write([]byte(`{"ok":true}`))
}
