# SONIQ SIP Architecture Pivot — State Doc
# Date: 8 May 2026

## The Big Idea

Deskphones become native LiveKit SIP endpoints.
No PBX. No Drachtio. No B2BUA. No sip-node.

> Connectivity already exists. INVITE creates orchestration.

## What We're Building

Fork of livekit/sip with three additions:

1. **REGISTER handler** (new file: `pkg/sip/registrar.go`)
   - SIPGo `srv.OnRegister(s.onRegister)` in server.go Start()
   - Digest auth against Supabase `sip_credentials` table
   - Device allowlist from `sip_devices` table
   - NAT contact stored in Redis: `sip:endpoint:{identity}`
   - Dual registration across both nodes (Yealink Server1/Server2)

2. **Endpoint resolution in outbound** (modify: `pkg/sip/outbound.go`)
   - Before looking up a trunk, check `sip:endpoint:{callTo}` in Redis
   - If found, INVITE goes direct to phone's NAT contact
   - No trunk needed for registered deskphones

3. **Action URL HTTP handler** (new file: `pkg/sip/actions.go`)
   - Mute → LiveKit muteParticipant
   - DND → Ably presence update
   - Hold → LiveKit mute + Ably state
   - Transfer → LiveKit room API
   - Yealink DSS keys configured via CFG provisioning

## Repo

- Fork: github.com/jonnyweareone/sip
- Branch: soniq
- Local: /Users/davidsmith/Documents/GitHub/livekit-sip-soniq
- Upstream: github.com/livekit/sip (pull periodically)

## Architecture

```
Port 5060 (UDP) → LiveKit SIP (PSTN trunks: OneHub, Twilio)
Port 5080 (TLS) → LiveKit SIP SONIQ (deskphone registration)
Same binary, two listeners.
Same Redis (Upstash), same LiveKit server.
```

## Key Source Files to Modify

| File | Size | Change |
|------|------|--------|
| pkg/sip/server.go | 9.6KB | Add `s.sipSrv.OnRegister(s.onRegister)`, add 5080 TLS listener |
| pkg/sip/registrar.go | NEW | REGISTER handler, digest auth, Redis endpoint store |
| pkg/sip/outbound.go | 35KB | Check registered endpoints before trunk lookup |
| pkg/sip/actions.go | NEW | HTTP action URL handler for button mappings |
| pkg/sip/config.go | 3.2KB | Add registration port, Supabase config |
| pkg/config/*.go | | Add SONIQ-specific config fields |

## Provisioning Chain

```
Yealink boots → RPS → SONIQ provisioning URL
→ CFG download:
  - SIP Server 1: sip-lhr-1.soniqlabs.co.uk:5080 (TLS)
  - SIP Server 2: sip-man-1.soniqlabs.co.uk:5080 (TLS)
  - TLS CA cert URL: /api/provisioning/cert/ca
  - Action URLs for every button
  - LiveKit identity binding
```

## Identity Model

`1002.soniq-master` is the same identity whether on:
- Yealink deskphone (SIP, registered on 5080)
- Web app (WebRTC, connected to LiveKit)
- Mobile app (WebRTC, connected to LiveKit)

When soniq-router says "ring 1002.soniq-master", LiveKit checks:
- Registered SIP endpoint? → INVITE to NAT contact
- Connected WebRTC participant? → Room invite
- Ring all simultaneously

## BLF / Presence

No SIP SUBSCRIBE/NOTIFY infrastructure needed.
- Ably fires presence change
- HTTPS XML push to phone's action URL
- Or Yealink XML browser polling endpoint
- State: idle/ringing/busy/offline derived from LiveKit participant state

## What Gets Retired

- Drachtio server (docker container)
- soniq-sip-node (TypeScript, npm, all the build issues)
- drachtio-srf library
- B2BUA call ownership
- SIP dialog state management
- drachtio-inbound/drachtio-outbound trunks
- All the res.send(200) bugs, ws compat issues, yanked packages

## Current Infrastructure

Both nodes (sip-lhr-1, sip-man-1):
- LiveKit server: stock upstream, host networking
- LiveKit SIP: will be replaced by our fork
- Drachtio: WILL BE REMOVED
- soniq-sip-node: WILL BE REMOVED

## Redis Keys (new)

```
sip:endpoint:{identity} → hash {
  contact_uri: "sip:1002.soniq-master@84.9.211.231:5060;transport=tls"
  nat_ip: "84.9.211.231"
  nat_port: "5060"
  transport: "tls"
  node_id: "lhr-1"
  mac: "805ec03e2c49"
  user_agent: "Yealink SIP-T58 58.85.0.5"
  registered_at: 1778247000
  expires_at: 1778250600
  org_id: "a0000000-..."
  livekit_identity: "1002.soniq-master"
}
```

## Next Session Prompt

"Continue building SONIQ LiveKit SIP fork. Repo at /Users/davidsmith/Documents/GitHub/livekit-sip-soniq on branch soniq. Need to: (1) add registrar.go with REGISTER handler + digest auth + Redis endpoint storage, (2) add OnRegister to server.go Start(), (3) add registration port 5080 TLS config, (4) modify outbound.go to resolve registered endpoints. Reference the architecture doc and state doc."
