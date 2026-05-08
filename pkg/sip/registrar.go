// SONIQ SIP Registrar — Persistent deskphone endpoints in LiveKit
//
// Handles SIP REGISTER on port 5080 (mutual TLS). Authentication is via
// client certificate — only SONIQ-provisioned phones have a cert signed
// by the SONIQ CA (delivered via CFG provisioning chain).
//
// The TLS handshake IS the auth. No digest challenge. No passwords.
// Identity is extracted from the certificate CN (= LiveKit identity).
//
// On successful REGISTER:
//   1. Extract identity from verified client cert CN
//   2. Verify username exists + enabled in Supabase sip_credentials
//   3. Store NAT contact in Redis as sip:endpoint:{identity}
//   4. Phone is now a persistent LiveKit endpoint, invitable to any room
//
// Re-REGISTER every 60s refreshes the Redis TTL (keepalive).
// REGISTER with Expires: 0 removes the endpoint (de-register).

package sip

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/livekit/protocol/logger"
	"github.com/livekit/sipgo/sip"
	goredis "github.com/redis/go-redis/v9"

	"github.com/livekit/sip/pkg/config"
)
const (
	redisEndpointPrefix = "sip:endpoint:"
)

// sipCredential maps to the Supabase sip_credentials table.
type sipCredential struct {
	ID       string `json:"id"`
	OrgID    string `json:"org_id"`
	Username string `json:"username"` // e.g. "1002.soniq-master" = LiveKit identity
	Enabled  bool   `json:"enabled"`
}

// Registrar handles SIP REGISTER for SONIQ deskphones.
// Auth is via mutual TLS client certificate — no passwords.
type Registrar struct {
	log    logger.Logger
	conf   *config.Config
	soniq  *config.SONIQConfig
	redis  goredis.UniversalClient
	client *http.Client
}

func NewRegistrar(conf *config.Config, log logger.Logger, rc goredis.UniversalClient) *Registrar {
	return &Registrar{
		log:    log.WithValues("component", "soniq-registrar"),
		conf:   conf,
		soniq:  conf.SONIQ,
		redis:  rc,
		client: &http.Client{Timeout: 5 * time.Second},
	}
}

// OnRegister is the sipgo handler wired in server.go Start().
// Client cert is already validated by Go's TLS stack (RequireAndVerifyClientCert).
// If we're here, the phone has a valid cert signed by the SONIQ CA.
func (reg *Registrar) OnRegister(req *sip.Request, tx sip.ServerTransaction) {
	ctx := context.Background()
	log := reg.log

	// Extract source IP (NAT contact)
	src, err := netip.ParseAddrPort(req.Source())
	if err != nil {
		log.Errorw("cannot parse REGISTER source", err)
		tx.Terminate()
		return
	}

	// Identity from From header username (e.g. "1002.soniq-master")
	// This matches the cert CN provisioned to the device.
	from := req.From()
	if from == nil || from.Address == nil {
		log.Warnw("REGISTER missing From header", nil)
		_ = tx.Respond(sip.NewResponseFromRequest(req, 400, "Bad Request", nil))
		return
	}
	identity := from.Address.User

	// Contact header (the phone's reachable address)
	contactHdr := req.GetHeader("Contact")

	// User-Agent
	uaHdr := req.GetHeader("User-Agent")
	userAgent := ""
	if uaHdr != nil {
		userAgent = uaHdr.Value()
	}

	// Expires
	expires := reg.soniq.RegExpiry
	if exHdr := req.GetHeader("Expires"); exHdr != nil {
		if v, err := strconv.Atoi(strings.TrimSpace(exHdr.Value())); err == nil {
			expires = v
		}
	}

	log = log.WithValues(
		"identity", identity,
		"source", src.String(),
		"userAgent", userAgent,
		"expires", expires,
	)

	log.Infow("REGISTER received")

	// --- De-registration (Expires: 0) ---
	if expires == 0 {
		key := redisEndpointPrefix + identity
		reg.redis.Del(ctx, key)
		_ = tx.Respond(sip.NewResponseFromRequest(req, 200, "OK", nil))
		log.Infow("endpoint de-registered")
		return
	}

	// --- Verify identity exists + enabled in Supabase ---
	dbCred, err := reg.lookupCredentials(ctx, identity)
	if err != nil {
		log.Errorw("supabase lookup failed", err)
		_ = tx.Respond(sip.NewResponseFromRequest(req, 500, "Server Error", nil))
		return
	}
	if dbCred == nil || !dbCred.Enabled {
		log.Warnw("identity not found or disabled", nil, "identity", identity)
		_ = tx.Respond(sip.NewResponseFromRequest(req, 403, "Forbidden", nil))
		return
	}

	// --- Store endpoint in Redis ---
	contactURI := fmt.Sprintf("sip:%s@%s;transport=tls", identity, src.String())
	if contactHdr != nil {
		contactURI = contactHdr.Value()
	}

	mac := extractMAC(userAgent)
	ttl := time.Duration(float64(expires)*1.5) * time.Second
	key := redisEndpointPrefix + identity

	fields := map[string]interface{}{
		"contact_uri":      contactURI,
		"nat_ip":           src.Addr().String(),
		"nat_port":         strconv.Itoa(int(src.Port())),
		"transport":        "tls",
		"node_id":          reg.soniq.NodeID,
		"mac":              mac,
		"user_agent":       userAgent,
		"registered_at":    strconv.FormatInt(time.Now().Unix(), 10),
		"expires_at":       strconv.FormatInt(time.Now().Add(ttl).Unix(), 10),
		"org_id":           dbCred.OrgID,
		"livekit_identity": identity,
	}

	pipe := reg.redis.Pipeline()
	pipe.HSet(ctx, key, fields)
	pipe.Expire(ctx, key, ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		log.Errorw("failed to store endpoint in Redis", err, "key", key)
		_ = tx.Respond(sip.NewResponseFromRequest(req, 500, "Server Error", nil))
		return
	}

	// 200 OK — one round trip, no challenge
	res := sip.NewResponseFromRequest(req, 200, "OK", nil)
	res.AppendHeader(sip.NewHeader("Expires", strconv.Itoa(expires)))
	_ = tx.Respond(res)

	log.Infow("endpoint registered",
		"orgID", dbCred.OrgID,
		"contact", contactURI,
		"ttl", ttl.String(),
	)
}

// lookupCredentials queries Supabase PostgREST for sip_credentials by username.
func (reg *Registrar) lookupCredentials(ctx context.Context, username string) (*sipCredential, error) {
	url := fmt.Sprintf("%s/rest/v1/sip_credentials?username=eq.%s&select=id,org_id,username,enabled&limit=1",
		reg.soniq.SupabaseURL, username)

	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("apikey", reg.soniq.SupabaseAnonKey)
	httpReq.Header.Set("Authorization", "Bearer "+reg.soniq.SupabaseAnonKey)

	resp, err := reg.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("supabase request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading supabase response: %w", err)
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("supabase returned %d: %s", resp.StatusCode, string(body))
	}

	var creds []sipCredential
	if err := json.Unmarshal(body, &creds); err != nil {
		return nil, fmt.Errorf("parsing credentials: %w", err)
	}
	if len(creds) == 0 {
		return nil, nil
	}
	return &creds[0], nil
}

// ResolveEndpoint looks up a registered deskphone by LiveKit identity.
func (reg *Registrar) ResolveEndpoint(ctx context.Context, identity string) (contactURI string, fields map[string]string, err error) {
	key := redisEndpointPrefix + identity
	result, err := reg.redis.HGetAll(ctx, key).Result()
	if err != nil {
		return "", nil, err
	}
	if len(result) == 0 {
		return "", nil, nil
	}
	return result["contact_uri"], result, nil
}

// extractMAC pulls a MAC address from a Yealink User-Agent string.
func extractMAC(ua string) string {
	parts := strings.Fields(ua)
	for _, p := range parts {
		if len(p) == 17 && strings.Count(p, ":") == 5 {
			return strings.ReplaceAll(p, ":", "")
		}
	}
	return ""
}
