# link

Bilateral sync primitive in Rust. A sequential channel between two peers — one request-response pair per counter tick, either side can initiate, turn bit resolves collisions. Requests and responses are persisted before sending, making retransmission idempotent.

Port of [github.com/jhnngrn/link](https://github.com/jhnngrn/link) (Go).

## What it provides

- **Link** — bilateral sync state for one connection: counter, turn bit, pending message, cached response, timestamp
- **Links** — thread-safe collection of links with policy, module dispatch, and the full sync loop (incoming, outgoing, retry, scheduling)
- **ConnectionState** — reusable state machine: Status + IntentConnect/IntentClose + reference counting
- **Module** — trait for application modules: handle_request, handle_response, handle_account, next_tx
- **EngineConfig** — callbacks the sync loop needs: Send, Commit, Now
- **Wire types** — SyncRequest, SyncResponse envelopes
- **PeerSend** — runtime send lifecycle tracking (in-flight, refractory, retry with cancel tokens)

## Usage

```rust
use link::{Links, EngineConfig, InviteMode, SyncRequest};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

// Engine callbacks — the app provides transport and persistence.
let engine = EngineConfig {
    send: Box::new(|peer, msg| {
        // POST msg to peer, return response body
        todo!()
    }),
    commit: Box::new(|| { /* serialize and persist */ }),
    now: Box::new(|| std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64),
    retry_delay: Duration::from_secs(60),
};

// Create a collection for node "alice"
let ls = Links::new("alice".into(), HashMap::new(), InviteMode::Invite, engine);

// Register application modules
ls.register_module(my_module);

// Incoming sync — Links handles locking, dispatch, persistence, kicks
let resp = ls.handle_sync(env)?;

// Account API — Links handles locking, commit, and kicks internally
let result = ls.handle_account("link.set_invite_mode", &payload)?;

// Resume pending sends at boot
ls.kick_all();
```

## Design

Links owns the full sync lifecycle: dispatch, locking, persistence (via Commit callback), outgoing sends (via Send callback), retry timers, and scheduling. Application modules register with Links and handle domain-specific protocol messages.

The sync protocol guarantees:

- **Sequential** — one decision in flight per link, shared counter
- **Crash-safe** — persisted before send, persisted before respond
- **Collision-free** — turn bit determines dominance, alternates each counter
- **Guaranteed delivery** — sender retries until response received

### Timestamps

The dominant peer (determined by `counter % 2 == turn_bit`) sets the authoritative timestamp. Non-dominant senders omit the timestamp; the dominant receiver substitutes its own clock. If the dominant clock is behind the floor (set by the previous dominant peer), it clamps to `max(now, floor)`. Timestamps are validated for monotonicity and a forward cap of 10 minutes.

### Thread safety

Links wraps its state in `Arc<Mutex<_>>`. Kick spawns a thread for each send attempt. Retry timers use `Arc`/`Weak` cancel tokens — replacing a PeerSend drops the old Arc, and the timer thread detects cancellation via the Weak reference.
