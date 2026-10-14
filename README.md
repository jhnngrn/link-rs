# link

Bilateral sync primitive in Rust. A sequential channel between two peers — one request-response pair per counter tick, either side can initiate, turn bit resolves collisions. Requests and responses are persisted before sending, making retransmission idempotent.

## What it does

- **Sync counter** — request/response pairs with monotonic counter, collision detection via turn bit, cached responses for retransmission
- **Connection lifecycle** — connect, close, ref-counting for graceful shutdown
- **Module negotiation** — bilateral opt-in to application modules that run over the link
- **Invite modes** — closed, invite-only, or open for incoming connections

## Usage

```rust
use link::{Links, Link, InviteMode};
use std::collections::HashMap;

// Create link collections for two nodes.
let mut alice = Links::new(
    "alice".into(),
    HashMap::new(),
    InviteMode::Invite,
    HashMap::new(),
);
let mut bob = Links::new(
    "bob".into(),
    HashMap::new(),
    InviteMode::Invite,
    HashMap::new(),
);

// Create links with intent to connect.
alice.create("bob").state.connection.intent_connect = true;
bob.create("alice").state.connection.intent_connect = true;

// Exchange connect messages — both sides are now Connected.
let r = bob.handle_request("alice", "connect", &serde_json::Value::Null).unwrap();
assert!(r.ok);
alice.handle_response("bob", "connect", &serde_json::Value::Null, true);
```
