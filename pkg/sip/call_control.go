// SONIQ Call Control — track active SIP invites for cancel/hangup
//
// invite-to-room stores call state + SIP Call-ID in Redis BEFORE sending INVITE.
// /api/call/cancel sends SIP BYE directly via persistent TLS using the stored Call-ID.
// /api/call/hangup does the same for established calls.

package sip

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/livekit/sipgo/sip"
)

const redisActiveCallPrefix = "sip:active:"

type callControlReq struct {
	Extension     string `json:"extension"`
	OrgSlug       string `json:"org_slug"`
	ParticipantID string `json:"participant_id"`
	RoomName      string `json:"room_name"`
	Reason        string `json:"reason"`
}

func (a *ActionServer) storeActiveCall(ctx context.Context, identity, participantID, roomName, state string) {
	key := redisActiveCallPrefix + identity
	a.redis.HSet(ctx, key, map[string]any{
		"participant_id": participantID,
		"room_name":      roomName,
		"state":          state,
		"created_at":     time.Now().Unix(),
	})
	a.redis.Expire(ctx, key, 2*time.Hour)
}

func (a *ActionServer) clearActiveCall(ctx context.Context, identity string) {
	a.redis.Del(ctx, redisActiveCallPrefix+identity)
}

func (a *ActionServer) getActiveCall(ctx context.Context, identity string) (map[string]string, error) {
	key := redisActiveCallPrefix + identity
	result, err := a.redis.HGetAll(ctx, key).Result()
	if err != nil || len(result) == 0 {
		return nil, fmt.Errorf("no active call for %s", identity)
	}
	return result, nil
}

func (a *ActionServer) handleCallHangup(w http.ResponseWriter, r *http.Request) {
	a.handleCallControl(w, r, "hangup")
}

func (a *ActionServer) handleCallCancel(w http.ResponseWriter, r *http.Request) {
	a.handleCallControl(w, r, "cancel")
}

func (a *ActionServer) handleCallControl(w http.ResponseWriter, r *http.Request, action string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	secret := r.Header.Get("X-Internal-Secret")
	if secret != "sNq-nTfY-2026-xK9p" {
		http.Error(w, "forbidden", 403)
		return
	}

	var req callControlReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), 400)
		return
	}
	if req.OrgSlug == "" {
		req.OrgSlug = "soniq-master"
	}

	identity := req.Extension + "." + req.OrgSlug
	ctx := r.Context()

	// Look up active call from Redis
	call, err := a.getActiveCall(ctx, identity)
	sipCallID := ""
	roomName := req.RoomName
	participantID := req.ParticipantID
	state := "unknown"

	if err == nil {
		sipCallID = call["sip_call_id"]
		if roomName == "" {
			roomName = call["room_name"]
		}
		if participantID == "" {
			participantID = call["participant_id"]
		}
		state = call["state"]
	}
	if participantID == "" {
		participantID = "sip-" + req.Extension
	}

	a.log.Infow("call "+action,
		"identity", identity,
		"room", roomName,
		"participant", participantID,
		"sip_call_id", sipCallID,
		"state", state,
		"reason", req.Reason,
	)

	// Send SIP BYE directly via persistent TLS connection
	byeSent := false
	if sipCallID != "" {
		err := a.sendSIPBye(ctx, identity, sipCallID)
		if err != nil {
			a.log.Errorw("direct SIP BYE failed", err, "identity", identity)
		} else {
			a.log.Infow("direct SIP BYE sent", "identity", identity, "call_id", sipCallID)
			byeSent = true
		}
	}

	a.clearActiveCall(ctx, identity)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":             true,
		"action":         action,
		"identity":       identity,
		"participant_id": participantID,
		"room_name":      roomName,
		"sip_call_id":    sipCallID,
		"bye_sent":       byeSent,
		"state":          state,
	})
}

// sendSIPBye sends a SIP BYE directly to the phone via persistent TLS connection.
// Uses the Call-ID from the original INVITE so the phone matches the dialog.
func (a *ActionServer) sendSIPBye(ctx context.Context, identity string, sipCallID string) error {
	if a.epWriter == nil {
		return fmt.Errorf("endpoint writer not available")
	}

	reqURI := sip.Uri{User: identity, Host: a.conf.Realm}
	bye := sip.NewRequest(sip.BYE, reqURI)

	via := &sip.ViaHeader{
		ProtocolName: "SIP", ProtocolVersion: "2.0", Transport: "TLS",
		Host: a.conf.ExternalIP, Port: a.conf.RegPortListen, Params: sip.NewParams(),
	}
	via.Params.Add("branch", sip.GenerateBranch())
	bye.AppendHeader(via)

	// Use the SAME Call-ID as the original INVITE so phone matches the dialog
	bye.AppendHeader(sip.NewHeader("From", fmt.Sprintf("<sip:soniq@%s>;tag=bye-%d", a.conf.Realm, time.Now().UnixMilli())))
	bye.AppendHeader(sip.NewHeader("To", fmt.Sprintf("<sip:%s@%s>", identity, a.conf.Realm)))
	bye.AppendHeader(sip.NewHeader("Call-ID", sipCallID))
	bye.AppendHeader(sip.NewHeader("CSeq", "2 BYE"))
	bye.AppendHeader(sip.NewHeader("Max-Forwards", "70"))
	bye.AppendHeader(sip.NewHeader("Content-Length", "0"))

	return a.epWriter.WriteMsg(ctx, identity, bye)
}
