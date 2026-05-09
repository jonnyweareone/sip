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

	"github.com/livekit/protocol/logger"
	"github.com/livekit/sipgo/sip"
	"github.com/livekit/sipgo/transport"
	goredis "github.com/redis/go-redis/v9"

	sipgo "github.com/livekit/sipgo"
)

// EndpointWriter writes SIP messages to registered phones via their
// persistent TLS registration connections.
type EndpointWriter struct {
	log   logger.Logger
	redis goredis.UniversalClient
	srv   *sipgo.Server // sipgo server — owns the TLS listener and connection pool
	mu    sync.RWMutex
}

func NewEndpointWriter(log logger.Logger, rc goredis.UniversalClient) *EndpointWriter {
	return &EndpointWriter{
		log:   log.WithValues("component", "soniq-endpoint-writer"),
		redis: rc,
	}
}

// SetServer wires the sipgo server reference (called after server starts).
func (ew *EndpointWriter) SetServer(srv *sipgo.Server) {
	ew.mu.Lock()
	defer ew.mu.Unlock()
	ew.srv = srv
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
