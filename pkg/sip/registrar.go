// SONIQ SIP Registrar — Persistent deskphone endpoints in LiveKit
//
// Handles SIP REGISTER on port 5080 (TLS). Authenticates via SIP digest auth
// against Supabase sip_credentials table (password_plain column).
// Same auth model as the original Drachtio registrar — TLS transport
// with digest credentials. The TLS cert chain lets the phone verify the
// server; the digest password lets the server verify the phone.
//
// On successful REGISTER:
//   1. Challenge with 401 + nonce (first REGISTER)
//   2. Verify digest response against password_plain from Supabase
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
	"sync"
	"time"

	"github.com/icholy/digest"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/sipgo/sip"
	goredis "github.com/redis/go-redis/v9"

	"github.com/livekit/sip/pkg/config"
)
const (
	redisEndpointPrefix = "sip:endpoint:"
	regChallengeLimit   = 1000
)

// sipCredential maps to Supabase sip_credentials table.
type sipCredential struct {
	ID            string  `json:"id"`
	OrgID         string  `json:"org_id"`
	Username      string  `json:"username"`       // "2000.workfones" = LiveKit identity
	PasswordPlain *string `json:"password_plain"`  // plaintext for digest auth
	DisplayName   *string `json:"display_name"`    // "Barry Kaye"
	Enabled       bool    `json:"enabled"`
}

// Registrar handles SIP REGISTER for SONIQ deskphones.
type Registrar struct {
	log    logger.Logger
	conf   *config.Config
	soniq  *config.SONIQConfig
	redis  goredis.UniversalClient
	client *http.Client

	mu         sync.Mutex
	challenges map[string]*regChallenge
}

type regChallenge struct {
	challenge digest.Challenge
	created   time.Time
}

func NewRegistrar(conf *config.Config, log logger.Logger, rc goredis.UniversalClient) *Registrar {
	return &Registrar{
		log:        log.WithValues("component", "soniq-registrar"),
		conf:       conf,
		soniq:      conf.SONIQ,
		redis:      rc,
		client:     &http.Client{Timeout: 5 * time.Second},
		challenges: make(map[string]*regChallenge),
	}
}

// OnRegister handles SIP REGISTER — digest auth, then Redis store.
func (reg *Registrar) OnRegister(req *sip.Request, tx sip.ServerTransaction) {
	ctx := context.Background()
	log := reg.log

	src, err := netip.ParseAddrPort(req.Source())
	if err != nil {
		log.Errorw("cannot parse REGISTER source", err)
		tx.Terminate()
		return
	}

	from := req.From()
	if from == nil {
		log.Warnw("REGISTER missing From header", nil)
		_ = tx.Respond(sip.NewResponseFromRequest(req, 400, "Bad Request", nil))
		return
	}
	identity := from.Address.User

	uaHdr := req.GetHeader("User-Agent")
	userAgent := ""
	if uaHdr != nil {
		userAgent = uaHdr.Value()
	}

	sipCallID := ""
	if h := req.CallID(); h != nil {
		sipCallID = h.Value()
	}

	expires := reg.soniq.RegExpiry
	if exHdr := req.GetHeader("Expires"); exHdr != nil {
		if v, err := strconv.Atoi(strings.TrimSpace(exHdr.Value())); err == nil {
			expires = v
		}
	}

	log = log.WithValues("identity", identity, "source", src.String(), "expires", expires)
	log.Infow("REGISTER received")

	// --- De-registration ---
	if expires == 0 {
		key := redisEndpointPrefix + identity
		reg.redis.Del(ctx, key)
		_ = tx.Respond(sip.NewResponseFromRequest(req, 200, "OK", nil))
		log.Infow("endpoint de-registered")
		return
	}

	// --- Digest auth: 401 challenge on first REGISTER ---
	authHdr := req.GetHeader("Authorization")
	if authHdr == nil {
		challenge := digest.Challenge{
			Realm:     reg.soniq.Realm,
			Nonce:     fmt.Sprintf("%d-%s", time.Now().UnixMicro(), sipCallID),
			Algorithm: "MD5",
		}
		reg.storeChallenge(sipCallID, challenge)
		res := sip.NewResponseFromRequest(req, 401, "Unauthorized", nil)
		res.AppendHeader(sip.NewHeader("WWW-Authenticate", challenge.String()))
		_ = tx.Respond(res)
		log.Debugw("sent 401 challenge")
		return
	}

	// --- Validate digest credentials ---
	cred, err := digest.ParseCredentials(authHdr.Value())
	if err != nil {
		log.Warnw("failed to parse Authorization", err)
		_ = tx.Respond(sip.NewResponseFromRequest(req, 401, "Bad credentials", nil))
		return
	}

	storedChallenge := reg.getChallenge(sipCallID)
	if storedChallenge == nil {
		log.Warnw("no challenge state", nil, "sipCallID", sipCallID)
		_ = tx.Respond(sip.NewResponseFromRequest(req, 401, "Stale nonce", nil))
		return
	}

	dbCred, err := reg.lookupCredentials(ctx, cred.Username)
	if err != nil {
		log.Errorw("supabase lookup failed", err)
		_ = tx.Respond(sip.NewResponseFromRequest(req, 500, "Server Error", nil))
		return
	}
	if dbCred == nil || !dbCred.Enabled || dbCred.PasswordPlain == nil {
		log.Warnw("unknown or disabled identity", nil, "username", cred.Username)
		_ = tx.Respond(sip.NewResponseFromRequest(req, 403, "Forbidden", nil))
		return
	}

	expected, err := digest.Digest(storedChallenge, digest.Options{
		Method:   "REGISTER",
		URI:      cred.URI,
		Username: cred.Username,
		Password: *dbCred.PasswordPlain,
	})
	if err != nil {
		log.Errorw("digest computation failed", err)
		_ = tx.Respond(sip.NewResponseFromRequest(req, 500, "Server Error", nil))
		return
	}
	if cred.Response != expected.Response {
		log.Warnw("digest auth failed", nil, "username", cred.Username)
		_ = tx.Respond(sip.NewResponseFromRequest(req, 403, "Forbidden", nil))
		return
	}

	// --- Auth passed — store endpoint in Redis ---
	// ALWAYS use the NAT source IP for the contact URI, never the phone's Contact header.
	// The phone's Contact contains its private LAN IP (192.168.0.x) which is unreachable
	// from the server. The NAT source IP (from Via received/rport) is the only way to
	// reach the phone. This is critical for sending INVITE to registered endpoints.
	contactURI := fmt.Sprintf("<sip:%s@%s;transport=TLS>", identity, src.String())
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
		log.Errorw("Redis store failed", err, "key", key)
		_ = tx.Respond(sip.NewResponseFromRequest(req, 500, "Server Error", nil))
		return
	}

	res := sip.NewResponseFromRequest(req, 200, "OK", nil)
	res.AppendHeader(sip.NewHeader("Expires", strconv.Itoa(expires)))
	_ = tx.Respond(res)
	log.Infow("endpoint registered",
		"orgID", dbCred.OrgID, "contact", contactURI, "ttl", ttl.String(),
	)
}

// lookupCredentials queries Supabase PostgREST for sip_credentials.
func (reg *Registrar) lookupCredentials(ctx context.Context, username string) (*sipCredential, error) {
	url := fmt.Sprintf("%s/rest/v1/sip_credentials?username=eq.%s&select=id,org_id,username,password_plain,display_name,enabled&limit=1",
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
		return nil, fmt.Errorf("reading response: %w", err)
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

func (reg *Registrar) storeChallenge(sipCallID string, ch digest.Challenge) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if len(reg.challenges) > regChallengeLimit {
		cutoff := time.Now().Add(-2 * time.Minute)
		for k, v := range reg.challenges {
			if v.created.Before(cutoff) {
				delete(reg.challenges, k)
			}
		}
	}
	reg.challenges[sipCallID] = &regChallenge{challenge: ch, created: time.Now()}
}

func (reg *Registrar) getChallenge(sipCallID string) *digest.Challenge {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	rc, ok := reg.challenges[sipCallID]
	if !ok {
		return nil
	}
	delete(reg.challenges, sipCallID)
	return &rc.challenge
}

func extractMAC(ua string) string {
	parts := strings.Fields(ua)
	for _, p := range parts {
		if len(p) == 17 && strings.Count(p, ":") == 5 {
			return strings.ReplaceAll(p, ":", "")
		}
	}
	return ""
}
