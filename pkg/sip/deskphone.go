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
	"strings"
	"time"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/rpc"
	"github.com/livekit/protocol/utils/guid"
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

// processRegisteredInvite handles an INVITE from a registered deskphone.
// This bypasses trunk auth and dispatch rules — the phone is already trusted.
// Returns true if handled, false if caller is not a registered endpoint
// (in which case processInvite falls through to normal trunk logic).
func (s *Server) processRegisteredInvite(
	ctx context.Context,
	cc *sipInbound,
	from, to URI,
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

	// Ring the caller (they hear ringback)
	cc.StartRinging()

	// Resolve callee's NAT contact from Redis
	calleeContact, calleeFields, err := s.registrar.ResolveEndpoint(ctx, calleeIdentity)
	if err != nil || calleeContact == "" {
		log.Errorw("callee not registered", err, "callee", calleeIdentity)
		cc.RespondAndDrop(480, "Temporarily Unavailable")
		return
	}

	// Invite callee into the room via CreateSIPParticipant
	// This sends an INVITE to the callee's deskphone
	calleeAddr := calleeFields["nat_ip"] + ":" + calleeFields["nat_port"]

	go func() {
		_, err := s.cli.CreateSIPParticipant(ctx, &rpc.InternalCreateSIPParticipantRequest{
			SipCallId:             guid.New("SCL_"),
			Address:               calleeAddr,
			Transport:             livekit.SIPTransport_SIP_TRANSPORT_TLS,
			CallTo:                calleeIdentity,
			Number:                callerIdentity,
			RoomName:              roomName,
			ParticipantIdentity:   calleeIdentity,
			ParticipantName:       calleeIdentity,
			WaitUntilAnswered:     false,
		})
		if err != nil {
			log.Errorw("failed to invite callee", err)
		} else {
			log.Infow("callee invited", "callee", calleeIdentity, "address", calleeAddr)
		}
	}()

	// Now accept the caller's INVITE and join them to the same room.
	// We use DispatchAccept with the room config to let the existing
	// inbound call machinery handle media bridging.
	// This is done by returning the dispatch result back to processInvite.
	// But since we're handling this ourselves, we need to directly join.

	// For now: accept the call and connect caller to the room.
	// The inbound call machinery needs the dispatch result.
	// We store it so processInvite can pick it up.
	cc.soniqDispatch = &CallDispatch{
		Result:    DispatchAccept,
		ProjectID: orgID,
		Room: RoomConfig{
			RoomName: roomName,
			Participant: ParticipantConfig{
				Identity: callerIdentity,
				Name:     callerIdentity,
			},
		},
		EnabledFeatures: []livekit.SIPFeature{},
		RingingTimeout:  30 * time.Second,
		MaxCallDuration: 4 * time.Hour,
		MediaConfig:     &livekit.SIPMediaConfig{},
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
	callerNumber := "" // TODO: look up org's CLI from sip_credentials/org settings

	// Look up the org's outbound CLI
	if s.registrar != nil {
		cred, err := s.registrar.lookupCredentials(ctx, callerIdentity)
		if err == nil && cred != nil {
			// Use the extension part as the from number for now
			parts := strings.SplitN(cred.Username, ".", 2)
			if len(parts) > 0 {
				callerNumber = parts[0]
			}
		}
	}
	if callerNumber == "" {
		callerNumber = callerIdentity
	}

	// Invite the PSTN side via CreateSIPParticipant
	go func() {
		_, err := s.cli.CreateSIPParticipant(ctx, &rpc.InternalCreateSIPParticipantRequest{
			SipCallId:           guid.New("SCL_"),
			Address:             trunkAddr,
			Transport:           livekit.SIPTransport_SIP_TRANSPORT_UDP,
			CallTo:              destination,
			Number:              callerNumber,
			RoomName:            roomName,
			ParticipantIdentity: fmt.Sprintf("pstn-%s", destination),
			ParticipantName:     destination,
			WaitUntilAnswered:   false,
		})
		if err != nil {
			log.Errorw("failed to create PSTN participant", err)
		} else {
			log.Infow("PSTN participant created", "destination", destination, "trunk", trunkAddr)
		}
	}()

	// Accept the caller and join them to the room
	cc.soniqDispatch = &CallDispatch{
		Result:    DispatchAccept,
		ProjectID: orgID,
		Room: RoomConfig{
			RoomName: roomName,
			Participant: ParticipantConfig{
				Identity: callerIdentity,
				Name:     callerIdentity,
			},
		},
		EnabledFeatures: []livekit.SIPFeature{},
		RingingTimeout:  60 * time.Second,
		MaxCallDuration: 4 * time.Hour,
		MediaConfig:     &livekit.SIPMediaConfig{},
	}
}
