# link

Crash-safe bilateral channel with a shared counter — one decision in flight at a time, so races never arise. Persist before send, persist before respond, retry until delivered. Application modules plug in and write plain request/response logic; link handles ordering, collisions, and crash recovery.

Port of [github.com/jhnngrn/link](https://github.com/jhnngrn/link) (Go).

## What it provides

- **Link** — bilateral sync state for one connection: counter, turn bit, pending message, cached response, timestamp
- **Links** — thread-safe collection of links with policy, module dispatch, and the full sync loop (incoming, outgoing, retry, scheduling)
- **Module** — trait for application modules: handle_request, handle_response, handle_account, next_tx
- **Wire types** — SyncRequest, SyncResponse envelopes
- **PeerSend** — runtime send lifecycle tracking (in-flight, retry with cancel tokens)

## Usage

```rust
use link::{Links, InviteMode, SyncRequest};
use serde_json::value::RawValue;
use std::collections::HashMap;

// Create a collection for node "alice"
let ls = Links::new(
    "alice".into(),
    HashMap::new(),
    InviteMode::Invite,
    |peer, msg| {
        // POST msg to peer, return response body
        todo!()
    },
    || { /* serialize and persist */ },
);

// Register application modules
ls.register_module(my_module);

// Incoming sync — Links handles locking, dispatch, persistence, kicks
let resp = ls.handle_sync(env)?;

// Account API — payload is &RawValue (opaque JSON)
let payload = RawValue::from_string(r#"{"mode":"open"}"#.into()).unwrap();
let result = ls.handle_account("link.set_invite_mode", &payload)?;

// Resume pending sends at boot
ls.kick_all();
```

## Design

Links owns the full sync lifecycle: dispatch, locking, persistence (via Commit callback), outgoing sends (via Send callback), retry timers, and scheduling. Application modules register with Links and handle domain-specific protocol messages. The pattern follows kernel TCP — the protocol package owns routing and the loop, modules are like sockets dispatched to.

The sync protocol guarantees:

- **Sequential** — one decision in flight per link, shared counter
- **Crash-safe** — persisted before send, persisted before respond
- **Collision-free** — turn bit determines dominance, alternates each counter
- **Guaranteed delivery** — sender retries until response received

### Modules

A module is anything that implements the `Module` trait:

```rust
pub trait Module: Send + Sync {
    fn name(&self) -> &str;

    // Incoming request from peer. Return (ok, kicks).
    // ok=true accepts, ok=false NACKs. kicks lists peers to send to next.
    fn handle_request(&mut self, peer: &str, method: &str,
        payload: &RawValue, ts: i64) -> Result<(bool, Vec<String>), LinkError>;

    // Response to our outgoing request. ok reflects the peer's accept/reject.
    // Return kicks.
    fn handle_response(&mut self, peer: &str, method: &str,
        payload: &RawValue, ok: bool, ts: i64) -> Vec<String>;

    // Admin/account operation (not sync). Return (response, kicks).
    fn handle_account(&mut self, method: &str,
        payload: &RawValue) -> Result<(Box<RawValue>, Vec<String>), LinkError>;

    // Pull next outgoing message for peer: (method, payload, queued_at).
    // None means nothing to send. queued_at is unix seconds —
    // when multiple modules have a message for the same peer, oldest wins.
    // Links polls this — the module decides internally what's next:
    // a queue, a state machine, whatever fits.
    fn next_tx(&self, peer: &str) -> Option<(String, Box<RawValue>, i64)>;
}
```

Payloads are `Box<RawValue>` — validated but unparsed JSON. The link layer never inspects them; modules parse what they need on their end. Modules don't persist — Links calls `Commit` after every state change.

### Connection lifecycle

```
  create(peer)              incoming link.connect
      |                           |
      v                           v
  +---------+              +----------+
  | waiting |              | incoming |
  +----+----+              +-----+----+
       |                         |
  link.connect              accept_link
  mutual or ACK             (intent_connect)
       |                         |
       v                         v
  +-----------+            +-----------+
  | connected | <----------| connected |
  +-----------+            +-----------+
        |
   intent_close
   (refs==0)
        |
   link.close ACK
        |
        v
    (deleted)
```

Modules use the same ConnectionState pattern. Close is deferred while refs > 0 — modules finish their work before the link closes.

### Sync loop

Links runs the outgoing sync loop internally:

- **kick(peer)** — triggers a send attempt for a peer (spawns thread)
- **kick_all()** — kicks all peers, used at boot to resume pending work
- **schedule(delay, fn)** — runs fn after delay under the Links mutex, commits and kicks any peers fn returns. General-purpose deferred operation — not tied to any specific link or module
- **send_pending(peer)** — acquires the lock, calls next_tx, sends via Send, handles the response, retries on failure
- **quick_reject(from, method)** — fast pre-auth rejection before expensive signature verification
- **handle_sync(env)** — incoming sync: validates, dispatches to modules, persists, kicks

### Timestamps

The dominant peer (determined by `counter % 2 == turn_bit`) sets the authoritative timestamp. Non-dominant senders omit the timestamp; the dominant receiver substitutes its own clock. If the dominant clock is behind the floor (set by the previous dominant peer), it clamps to `max(now, floor)`. Timestamps are validated for monotonicity and a forward cap of 10 minutes.

### Thread safety

Links wraps its state in `Arc<Mutex<_>>`. Kick spawns a thread for each send attempt. Retry timers use `Arc`/`Weak` cancel tokens — replacing a PeerSend drops the old Arc, and the timer thread detects cancellation via the Weak reference.
