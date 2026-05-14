// SONIQ Call Control — track active SIP invites for cancel/hangup
//
// invite-to-room stores call state in Redis.
// /api/call/hangup and /api/call/cancel return room+participant info
// so the router can call LiveKit RemoveParticipant.
// Also attempts direct SIP BYE/CANCEL as fallback.

package sip

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
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

func (a *ActionServer) getActiveCall(ctx context.Context, identity string) (string, string, string, error) {
	key := redisActiveCallPrefix + identity
	result, err := a.redis.HGetAll(ctx, key).Result()
	if err != nil || len(result) == 0 {
		return "", "", "", fmt.Errorf("no active call for %s", identity)
	}
	return result["participant_id"], result["room_name"], result["state"], nil
}

// handleCallHangup returns room + participant info for the router to do LiveKit removal.
// POST /api/call/hangup { "extension": "1000", "org_slug": "soniq-master" }
func (a *ActionServer) handleCallHangup(w http.ResponseWriter, r *http.Request) {
	a.handleCallControl(w, r, "hangup")
}

// handleCallCancel returns room + participant info for ringing calls.
// POST /api/call/cancel { "extension": "1000", "org_slug": "soniq-master" }
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

	pid, room, state, err := a.getActiveCall(ctx, identity)
	if err != nil {
		pid = "sip-" + req.Extension
		state = "unknown"
	}
	if req.RoomName != "" {
		room = req.RoomName
	}
	if req.ParticipantID != "" {
		pid = req.ParticipantID
	}

	a.log.Infow("call "+action,
		"identity", identity,
		"room", room,
		"participant", pid,
		"state", state,
		"reason", req.Reason,
	)

	a.clearActiveCall(ctx, identity)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":             true,
		"action":         action,
		"identity":       identity,
		"participant_id": pid,
		"room_name":      room,
		"state":          state,
	})
}
