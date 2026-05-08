// SONIQ Deskphone Call Routing — handles INVITE from registered endpoints
//
// When a registered deskphone dials a number, the INVITE arrives on port 5080.
// This handler bypasses trunk auth and dispatch rules entirely.
// Instead it:
//   - Verifies the caller is a registered endpoint (Redis lookup)
//   - Determines if the destination is internal (another registered endpoint)
//     or external (PSTN number)
//   - For internal: creates a LiveKit room, invites both SIP endpoints
//   - For external: creates a room, bridges caller SIP to PSTN trunk via
//     CreateSIPParticipant with LCR-selected trunk
//
// The caller's SIP session is already established (they sent the INVITE).
// The existing inbound call machinery handles the media bridging.
// We just need to provide the right room config and invite the callee.

package sip

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	pionsdp "github.com/pion/sdp/v3"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/rpc"
	"github.com/livekit/protocol/utils/guid"
	"github.com/livekit/sipgo/sip"
)

// isRegisteredEndpoint checks if the caller is a registered SONIQ deskphone.
// Returns the endpoint fields from Redis if found, nil if not.
func (s *Server) isRegisteredEndpoint(ctx context.Context, identity string) map[string]string {
	if s.registrar == nil {
		return nil
	}
	_, fields, err := s.registrar.ResolveEndpoint(ctx, identity)
	if err != nil || len(fields) == 0 {
		return nil
	}
	return fields
}

// extensionFromIdentity strips the org slug from a LiveKit identity.
// "1003.soniq-master" → "1003"
func extensionFromIdentity(identity string) string {
	if idx := strings.Index(identity, "."); idx > 0 {
		return identity[:idx]
	}
	return identity
}

// displayName returns "Jonny Robinson (1000)" for display on the callee's phone.
func (s *Server) displayName(ctx context.Context, identity string) string {
	ext := extensionFromIdentity(identity)
	if s.registrar == nil {
		return ext
	}
	cred, err := s.registrar.lookupCredentials(ctx, identity)
	if err != nil || cred == nil || cred.DisplayName == nil || *cred.DisplayName == "" {
		return ext
	}
	return fmt.Sprintf("%s (%s)", *cred.DisplayName, ext)
}

// processRegisteredInvite handles an INVITE from a registered deskphone.
// This bypasses trunk auth and dispatch rules — the phone is already trusted.
// Returns true if handled, false if caller is not a registered endpoint
// (in which case processInvite falls through to normal trunk logic).
func (s *Server) processRegisteredInvite(
	ctx context.Context,
	cc *sipInbound,
	from, to sip.Uri,
	log logger.Logger,
) bool {
	callerIdentity := from.User
	callerFields := s.isRegisteredEndpoint(ctx, callerIdentity)
	if callerFields == nil {
		return false // not a registered endpoint, fall through to trunk logic
	}

	orgID := callerFields["org_id"]
	dialledNumber := to.User
	log = log.WithValues(
		"soniqCall", true,
		"caller", callerIdentity,
		"dialled", dialledNumber,
		"orgID", orgID,
	)
	log.Infow("deskphone INVITE — registered endpoint call")

	// Store caller's NAT public IP for SDP rewriting
	cc.natPublicIP = callerFields["nat_ip"]

	// Send 100 Trying
	cc.Processing()

	// Determine: internal extension or external PSTN?
	// Internal = the dialled number resolves to a registered endpoint or
	// matches {ext}.{slug} pattern for same org
	calleeIdentity := s.resolveCallee(ctx, dialledNumber, orgID, log)

	if calleeIdentity != "" {
		// ── Internal call ──
		log.Infow("routing internal call", "callee", calleeIdentity)
		s.handleInternalCall(ctx, cc, callerIdentity, calleeIdentity, orgID, log)
	} else {
		// ── External call (PSTN) ──
		log.Infow("routing external call", "destination", dialledNumber)
		s.handleExternalCall(ctx, cc, callerIdentity, dialledNumber, orgID, log)
	}
	return true
}

// resolveCallee determines if the dialled number is an internal extension.
// Checks Redis for registered endpoint, and tries {ext}.{slug} pattern.
// Returns the LiveKit identity if internal, empty string if external.
func (s *Server) resolveCallee(ctx context.Context, dialled string, orgID string, log logger.Logger) string {
	if s.registrar == nil {
		return ""
	}

	// Direct identity match (e.g. user dialled "1003.soniq-master")
	_, fields, _ := s.registrar.ResolveEndpoint(ctx, dialled)
	if len(fields) > 0 && fields["org_id"] == orgID {
		return dialled
	}

	// Extension-only match: scan Redis for sip:endpoint:* where identity ends with
	// the right extension. This is a simple approach — in production we'd query
	// Supabase for the org's extensions. For now, try common patterns.
	// The org slug is embedded in the identity: {ext}.{slug}
	// We need to find what slug this org uses.
	// Quick approach: lookup caller's identity to extract slug, then try {dialled}.{slug}
	// The caller's identity IS in the callerFields we already have.
	// But we don't pass it here. Let's try a Redis scan.

	// Try prefix scan: look for any endpoint whose identity starts with "{dialled}."
	keys, err := s.registrar.redis.Keys(ctx, fmt.Sprintf("sip:endpoint:%s.*", dialled)).Result()
	if err == nil {
		for _, key := range keys {
			fields, _ := s.registrar.redis.HGetAll(ctx, key).Result()
			if len(fields) > 0 && fields["org_id"] == orgID {
				return fields["livekit_identity"]
			}
		}
	}

	return "" // external number
}

// handleInternalCall creates a LiveKit room and invites both endpoints.
// The caller is already connected via the inbound SIP session.
// The callee gets invited via CreateSIPParticipant using their Redis contact.
func (s *Server) handleInternalCall(
	ctx context.Context,
	cc *sipInbound,
	callerIdentity, calleeIdentity, orgID string,
	log logger.Logger,
) {
	roomName := fmt.Sprintf("call-%s-%s-%d", callerIdentity, calleeIdentity, time.Now().Unix())
	callID := guid.New("SCL_")

	log = log.WithValues("room", roomName, "callID", callID)
	log.Infow("creating internal call room")

	// Resolve callee's NAT contact from Redis
	calleeContact, calleeFields, err := s.registrar.ResolveEndpoint(ctx, calleeIdentity)
	if err != nil || calleeContact == "" {
		log.Errorw("callee not registered", err, "callee", calleeIdentity)
		cc.RespondAndDrop(480, "Temporarily Unavailable")
		return
	}

	calleeAddr := calleeFields["nat_ip"] + ":" + calleeFields["nat_port"]
	callerExt := extensionFromIdentity(callerIdentity)
	callerDisplay := s.displayName(ctx, callerIdentity)
	calleeDisplay := s.displayName(ctx, calleeIdentity)

	// Send 180 Ringing to the caller — they hear ringback tone while we invite the callee
	cc.StartRinging()
	log.Infow("ringing caller while inviting callee", "calleeAddr", calleeAddr)

	// Invite callee SYNCHRONOUSLY — caller hears ringback until callee answers.
	// CreateSIPParticipant blocks until the callee's phone answers (200 OK)
	// or times out. The caller's TLS connection stays open with 180 Ringing.
	callerDisplayName := callerDisplay
	_, err = s.cli.CreateSIPParticipant(ctx, &rpc.InternalCreateSIPParticipantRequest{
		SipCallId:           guid.New("SCL_"),
		Address:             calleeAddr,
		Transport:           livekit.SIPTransport_SIP_TRANSPORT_TLS,
		CallTo:              calleeIdentity,
		Number:              callerExt,
		DisplayName:         &callerDisplayName,
		RoomName:            roomName,
		ParticipantIdentity: calleeIdentity,
		ParticipantName:     calleeDisplay,
		WaitUntilAnswered:   true,
	})
	if err != nil {
		log.Errorw("callee did not answer", err, "callee", calleeIdentity)
		cc.RespondAndDrop(480, "Temporarily Unavailable")
		return
	}
	log.Infow("callee answered, accepting caller", "callee", calleeIdentity)

	// Callee answered — now accept the caller and join them to the same room
	cc.soniqDispatch = &CallDispatch{
		Result:    DispatchAccept,
		ProjectID: orgID,
		Room: RoomConfig{
			RoomName: roomName,
			Participant: ParticipantConfig{
				Identity: callerIdentity,
				Name:     callerDisplay,
			},
		},
		EnabledFeatures: []livekit.SIPFeature{},
		RingingTimeout:  30 * time.Second,
		MaxCallDuration: 4 * time.Hour,
		// Phone offers RTP/SAVP when using TLS transport — must match with SRTP
		MediaConfig: &livekit.SIPMediaConfig{
			Encryption: livekit.SIPMediaEncryption_SIP_MEDIA_ENCRYPT_REQUIRE.Enum(),
		},
	}
}

// handleExternalCall routes a PSTN call from a registered deskphone.
// Creates a LiveKit room, connects the caller, and bridges to PSTN trunk.
func (s *Server) handleExternalCall(
	ctx context.Context,
	cc *sipInbound,
	callerIdentity, destination, orgID string,
	log logger.Logger,
) {
	roomName := fmt.Sprintf("call-%s-%s-%d",
		callerIdentity,
		strings.ReplaceAll(destination, "+", ""),
		time.Now().Unix(),
	)

	log = log.WithValues("room", roomName)
	log.Infow("creating external call room")

	cc.StartRinging()

	// TODO: LCR lookup for cheapest trunk
	// For now, use OneHub as default outbound trunk
	trunkAddr := "34.147.235.69:5060" // OneHub
	callerDisplay := s.displayName(ctx, callerIdentity)

	// Outbound CLI — must be a real number the trunk accepts.
	// TODO: look up org's outbound CLI from org_settings or sip_trunks table
	callerNumber := "+442046283328" // SONIQ main number for now

	// Invite the PSTN side SYNCHRONOUSLY — caller hears ringback until remote answers
	log.Infow("ringing caller while inviting PSTN", "trunk", trunkAddr, "cli", callerNumber)
	_, err := s.cli.CreateSIPParticipant(ctx, &rpc.InternalCreateSIPParticipantRequest{
		SipCallId:           guid.New("SCL_"),
		Address:             trunkAddr,
		Transport:           livekit.SIPTransport_SIP_TRANSPORT_UDP,
		CallTo:              destination,
		Number:              callerNumber,
		RoomName:            roomName,
		ParticipantIdentity: fmt.Sprintf("pstn-%s", destination),
		ParticipantName:     destination,
		WaitUntilAnswered:   true,
	})
	if err != nil {
		log.Errorw("PSTN call failed", err, "destination", destination)
		cc.RespondAndDrop(503, "Service Unavailable")
		return
	}
	log.Infow("PSTN answered, accepting caller", "destination", destination)

	// PSTN answered — now accept the caller
	cc.soniqDispatch = &CallDispatch{
		Result:    DispatchAccept,
		ProjectID: orgID,
		Room: RoomConfig{
			RoomName: roomName,
			Participant: ParticipantConfig{
				Identity: callerIdentity,
				Name:     callerDisplay,
			},
		},
		EnabledFeatures: []livekit.SIPFeature{},
		RingingTimeout:  60 * time.Second,
		MaxCallDuration: 4 * time.Hour,
		// Phone offers RTP/SAVP when using TLS transport — must match with SRTP
		MediaConfig: &livekit.SIPMediaConfig{
			Encryption: livekit.SIPMediaEncryption_SIP_MEDIA_ENCRYPT_REQUIRE.Enum(),
		},
	}
}

// fixNATedSDP rewrites private IP addresses in SDP to the phone's public NAT IP.
// Uses pion/sdp for proper SDP parsing — handles all connection info fields
// at session level and per-media level.
//
// Every phone behind NAT advertises its private LAN IP in SDP.
// We replace with the public IP from the SIP Via received parameter.
// This is the Go equivalent of Kamailio's fix_nated_sdp().
func fixNATedSDP(sdpBytes []byte, publicIP string) []byte {
	if publicIP == "" {
		return sdpBytes
	}

	var sess pionsdp.SessionDescription
	if err := sess.Unmarshal(sdpBytes); err != nil {
		// Can't parse — return unchanged, string fallback
		return fixNATedSDPString(sdpBytes, publicIP)
	}

	changed := false

	// Fix session-level connection (c= line)
	if sess.ConnectionInformation != nil && sess.ConnectionInformation.Address != nil {
		addr := sess.ConnectionInformation.Address.Address
		if isPrivateIP(addr) {
			sess.ConnectionInformation.Address.Address = publicIP
			changed = true
		}
	}

	// Fix origin (o= line)
	if isPrivateIP(sess.Origin.UnicastAddress) {
		sess.Origin.UnicastAddress = publicIP
		changed = true
	}

	// Fix per-media connection info
	for i := range sess.MediaDescriptions {
		md := sess.MediaDescriptions[i]
		if md.ConnectionInformation != nil && md.ConnectionInformation.Address != nil {
			addr := md.ConnectionInformation.Address.Address
			if isPrivateIP(addr) {
				md.ConnectionInformation.Address.Address = publicIP
				changed = true
			}
		}
	}

	if !changed {
		return sdpBytes
	}

	out, err := sess.Marshal()
	if err != nil {
		return sdpBytes // marshal failed, return original
	}
	return out
}

// fixNATedSDPString is a string-based fallback if pion/sdp can't parse the SDP.
func fixNATedSDPString(sdpBytes []byte, publicIP string) []byte {
	lines := strings.Split(string(sdpBytes), "\r\n")
	for i, line := range lines {
		if strings.HasPrefix(line, "c=IN IP4 ") {
			oldIP := strings.TrimPrefix(line, "c=IN IP4 ")
			if isPrivateIP(oldIP) {
				lines[i] = "c=IN IP4 " + publicIP
			}
		}
		if strings.HasPrefix(line, "o=") && strings.Contains(line, "IN IP4 ") {
			parts := strings.Split(line, " IN IP4 ")
			if len(parts) == 2 && isPrivateIP(parts[1]) {
				lines[i] = parts[0] + " IN IP4 " + publicIP
			}
		}
	}
	return []byte(strings.Join(lines, "\r\n"))
}

// isPrivateIP checks if an IP string is a private/local address (RFC1918, link-local, loopback).
func isPrivateIP(ipStr string) bool {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		// Fall back to string prefix check
		return strings.HasPrefix(ipStr, "192.168.") ||
			strings.HasPrefix(ipStr, "10.") ||
			strings.HasPrefix(ipStr, "172.") ||
			strings.HasPrefix(ipStr, "169.254.") ||
			ipStr == "127.0.0.1" || ipStr == "0.0.0.0"
	}
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsUnspecified()
}
