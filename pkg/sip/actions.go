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
	log   logger.Logger
	conf  *config.SONIQConfig
	redis goredis.UniversalClient
	mux   *http.ServeMux
	srv   *http.Server
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

func (a *ActionServer) registerRoutes() {
	// POST /actions/{identity}/{action}
	// POST /actions/{identity}/execute-chip/{chip_id}
	// POST /actions/{identity}/dial/{target}
	// POST /actions/{identity}/xml-response/{prompt_id}/{key}
	a.mux.HandleFunc("/actions/", a.handleAction)
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
