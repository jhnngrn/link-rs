//! Bilateral sync primitive — data structures and protocol types for a single
//! link between two parties.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinkError {
    StaleCounter,
    CounterAhead,
    Collision,
    BadPayload,
    NoPeer(String),
    InvalidMode(String),
    RefsHeld,
    NoIncomingModule(String),
    UnknownMethod(String),
    Other(String),
}

impl fmt::Display for LinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StaleCounter => write!(f, "stale counter"),
            Self::CounterAhead => write!(f, "counter ahead"),
            Self::Collision => write!(f, "collision"),
            Self::BadPayload => write!(f, "bad payload"),
            Self::RefsHeld => write!(f, "close with refs > 0"),
            Self::NoPeer(p) => write!(f, "no link to {p}"),
            Self::InvalidMode(m) => write!(f, "invalid mode {m:?}"),
            Self::NoIncomingModule(m) => write!(f, "no incoming module {m}"),
            Self::UnknownMethod(m) => write!(f, "unknown link method {m:?}"),
            Self::Other(s) => write!(f, "{s}"),
        }
    }
}

impl std::error::Error for LinkError {}

// ---------------------------------------------------------------------------
// ConnectionStatus / ConnectionState
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConnectionStatus {
    Waiting,
    Incoming,
    Connected,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConnectionState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<ConnectionStatus>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub intent_connect: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub intent_close: bool,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub refs: u32,
}

fn is_zero(v: &u32) -> bool {
    *v == 0
}

impl ConnectionState {
    pub fn ref_inc(&mut self) {
        self.refs += 1;
    }

    pub fn unref(&mut self) {
        self.refs = self.refs.saturating_sub(1);
    }

    pub fn unref_close(&mut self) {
        self.refs = self.refs.saturating_sub(1);
        if self.refs == 0 {
            self.intent_close = true;
        }
    }
}

// ---------------------------------------------------------------------------
// LinkState
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LinkState {
    #[serde(flatten)]
    pub connection: ConnectionState,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub modules: HashMap<String, ConnectionState>,
}

// ---------------------------------------------------------------------------
// QueuedMsg
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedMsg {
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
}

// ---------------------------------------------------------------------------
// Wire envelopes
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncRequest {
    pub from: String,
    pub to: String,
    pub counter: u64,
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    pub timestamp: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resp: Option<SyncResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncResponse {
    pub counter: u64,
    pub ok: bool,
    pub timestamp: i64,
}

// ---------------------------------------------------------------------------
// Link
// ---------------------------------------------------------------------------

pub const MAX_LINKS: usize = 64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Link {
    pub state: LinkState,
    pub counter: u64,
    pub turn_bit: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending: Option<QueuedMsg>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached_response: Option<SyncResponse>,
    #[serde(default, rename = "ts")]
    pub timestamp: i64,
}

impl Link {
    pub fn new(self_id: &str, peer: &str) -> Self {
        let bit = if self_id > peer { 1 } else { 0 };
        Self {
            state: LinkState::default(),
            counter: 0,
            turn_bit: bit,
            pending: None,
            cached_response: None,
            timestamp: 0,
        }
    }

    /// Returns true when this side owns the current counter slot.
    pub fn dominant(&self) -> bool {
        self.counter % 2 == self.turn_bit
    }

    /// Returns true if the named module is bilaterally active on this link.
    pub fn module_enabled(&self, name: &str) -> bool {
        self.state
            .modules
            .get(name)
            .is_some_and(|c| c.status == Some(ConnectionStatus::Connected))
    }

    /// Validate an incoming request counter.
    /// Returns Ok(Some(resp)) for cached stale, Ok(None) for proceed, Err for invalid.
    pub fn validate_request(&self, counter: u64) -> Result<Option<SyncResponse>, LinkError> {
        if counter < self.counter {
            if let Some(ref cached) = self.cached_response {
                if cached.counter == counter {
                    return Ok(Some(cached.clone()));
                }
            }
            return Err(LinkError::StaleCounter);
        }
        if counter > self.counter {
            return Err(LinkError::CounterAhead);
        }
        if self.pending.is_some() && self.dominant() {
            return Err(LinkError::Collision);
        }
        Ok(None)
    }

    /// Finalize an incoming request. Returns the response to send back.
    pub fn accept_request(&mut self, req_counter: u64, ok: bool, ts: i64) -> SyncResponse {
        let resp = SyncResponse {
            counter: req_counter,
            ok,
            timestamp: ts,
        };
        self.cached_response = Some(resp.clone());
        self.counter += 1;
        self.timestamp = ts;
        self.pending = None;
        resp
    }

    /// Finalize a received response to our outgoing message.
    pub fn accept_response(&mut self, ts: i64) {
        self.counter += 1;
        self.timestamp = ts;
        self.pending = None;
    }

    // --- Module negotiation (internal) ---

    fn req_module(
        &mut self,
        payload: &Value,
        supported: &HashMap<String, ()>,
        invite_mode: InviteMode,
    ) -> Result<bool, LinkError> {
        let name = payload
            .get("name")
            .and_then(Value::as_str)
            .ok_or(LinkError::BadPayload)?;
        if name.is_empty() {
            return Err(LinkError::BadPayload);
        }
        if !supported.contains_key(name) {
            return Ok(false);
        }

        let name = name.to_string();
        let mod_state = self.state.modules.get(&name);

        // Mutual — we also want this module.
        if let Some(m) = mod_state {
            if m.intent_connect || m.status == Some(ConnectionStatus::Waiting) {
                let m = self.state.modules.get_mut(&name).unwrap();
                m.status = Some(ConnectionStatus::Connected);
                m.intent_connect = false;
                return Ok(true);
            }
        }

        // Open mode — auto-accept supported modules.
        if invite_mode == InviteMode::Open {
            let m = self
                .state
                .modules
                .entry(name)
                .or_insert_with(ConnectionState::default);
            m.status = Some(ConnectionStatus::Connected);
            return Ok(true);
        }

        // Mark as incoming — awaiting local accept/reject.
        let m = self
            .state
            .modules
            .entry(name)
            .or_insert_with(ConnectionState::default);
        m.status = Some(ConnectionStatus::Incoming);
        Ok(false) // NACK — not accepted yet
    }

    fn resp_module(&mut self, payload: &Value, ok: bool) {
        let Some(name) = payload.get("name").and_then(Value::as_str) else {
            return;
        };
        let Some(m) = self.state.modules.get_mut(name) else {
            return;
        };
        m.status = if ok {
            Some(ConnectionStatus::Connected)
        } else {
            Some(ConnectionStatus::Waiting)
        };
        m.intent_connect = false;
    }

    /// Propose the given modules on this link, filtering by supported.
    pub fn set_modules(&mut self, modules: &[String], supported: &HashMap<String, ()>) {
        let desired: HashMap<&str, ()> = modules
            .iter()
            .filter(|n| supported.contains_key(n.as_str()))
            .map(|n| (n.as_str(), ()))
            .collect();

        for name in desired.keys() {
            let name_s = name.to_string();
            let m = self
                .state
                .modules
                .entry(name_s)
                .or_insert_with(ConnectionState::default);
            if m.status != Some(ConnectionStatus::Connected) {
                m.intent_connect = true;
            }
        }

        let to_remove: Vec<String> = self
            .state
            .modules
            .iter()
            .filter(|(name, m)| {
                name.as_str() != "trustline"
                    && !desired.contains_key(name.as_str())
                    && m.status != Some(ConnectionStatus::Connected)
            })
            .map(|(name, _)| name.clone())
            .collect();
        for name in to_remove {
            self.state.modules.remove(&name);
        }
    }

    /// Accept an incoming module proposal.
    pub fn accept_module(&mut self, name: &str) -> Result<(), LinkError> {
        let m = self
            .state
            .modules
            .get_mut(name)
            .filter(|m| m.status == Some(ConnectionStatus::Incoming))
            .ok_or_else(|| LinkError::NoIncomingModule(name.to_string()))?;
        m.intent_connect = true;
        Ok(())
    }

    /// Reject an incoming module proposal.
    pub fn reject_module(&mut self, name: &str) -> Result<(), LinkError> {
        if self
            .state
            .modules
            .get(name)
            .is_none_or(|m| m.status != Some(ConnectionStatus::Incoming))
        {
            return Err(LinkError::NoIncomingModule(name.to_string()));
        }
        self.state.modules.remove(name);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// InviteMode
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InviteMode {
    Closed,
    Invite,
    Open,
}

impl Default for InviteMode {
    fn default() -> Self {
        Self::Invite
    }
}
// ---------------------------------------------------------------------------
// Links — collection of links for a node
// ---------------------------------------------------------------------------

/// Result of handle_request.
pub struct RequestResult {
    pub ok: bool,
}

pub struct Links {
    links: HashMap<String, Link>,
    self_id: String,
    pub invite_mode: InviteMode,
    pub supported: HashMap<String, ()>,
}

impl Links {
    pub fn new(
        self_id: String,
        links: HashMap<String, Link>,
        invite_mode: InviteMode,
        supported: HashMap<String, ()>,
    ) -> Self {
        Self {
            links,
            self_id,
            invite_mode,
            supported,
        }
    }

    pub fn get(&self, peer: &str) -> Option<&Link> {
        self.links.get(peer)
    }

    pub fn get_mut(&mut self, peer: &str) -> Option<&mut Link> {
        self.links.get_mut(peer)
    }

    pub fn len(&self) -> usize {
        self.links.len()
    }

    pub fn is_empty(&self) -> bool {
        self.links.is_empty()
    }

    pub fn full(&self) -> bool {
        self.links.len() >= MAX_LINKS
    }

    pub fn delete(&mut self, peer: &str) {
        self.links.remove(peer);
    }

    pub fn create(&mut self, peer: &str) -> &mut Link {
        let lk = Link::new(&self.self_id, peer);
        self.links.entry(peer.to_string()).or_insert(lk)
    }

    pub fn map(&self) -> &HashMap<String, Link> {
        &self.links
    }

    // --- Link-level request/response handling ---

    pub fn handle_request(
        &mut self,
        peer: &str,
        method: &str,
        payload: &Value,
    ) -> Result<RequestResult, LinkError> {
        let l = self
            .links
            .get_mut(peer)
            .ok_or_else(|| LinkError::NoPeer(peer.to_string()))?;

        match method {
            "connect" => {
                if l.state.connection.intent_connect
                    || l.state.connection.status == Some(ConnectionStatus::Waiting)
                {
                    l.state.connection.status = Some(ConnectionStatus::Connected);
                    l.state.connection.intent_connect = false;
                    return Ok(RequestResult { ok: true });
                }
                if self.invite_mode == InviteMode::Open {
                    l.state.connection.status = Some(ConnectionStatus::Connected);
                    return Ok(RequestResult { ok: true });
                }
                Ok(RequestResult { ok: false })
            }
            "close" => {
                if l.state.connection.refs > 0 {
                    return Err(LinkError::RefsHeld);
                }
                self.links.remove(peer);
                Ok(RequestResult { ok: true })
            }
            "module" => {
                let supported = self.supported.clone();
                let ok = l.req_module(payload, &supported, self.invite_mode)?;
                Ok(RequestResult { ok })
            }
            _ => Err(LinkError::UnknownMethod(method.to_string())),
        }
    }

    /// Processes a link-namespace response. Deletes the link on accepted close.
    pub fn handle_response(&mut self, peer: &str, method: &str, payload: &Value, ok: bool) {
        let Some(l) = self.links.get_mut(peer) else {
            return;
        };
        match method {
            "connect" => {
                l.state.connection.status = if ok {
                    Some(ConnectionStatus::Connected)
                } else {
                    Some(ConnectionStatus::Waiting)
                };
                l.state.connection.intent_connect = false;
            }
            "close" => {
                if ok {
                    self.links.remove(peer);
                } else {
                    l.state.connection.intent_close = false;
                }
            }
            "module" => {
                l.resp_module(payload, ok);
            }
            _ => {}
        }
    }

    /// Returns the next link-level message to send, or None.
    pub fn next_tx(&self, peer: &str) -> Option<QueuedMsg> {
        let l = self.links.get(peer)?;
        if l.state.connection.intent_close && l.state.connection.refs == 0 {
            return Some(QueuedMsg {
                method: "close".into(),
                payload: None,
            });
        }
        if l.state.connection.intent_connect {
            return Some(QueuedMsg {
                method: "connect".into(),
                payload: None,
            });
        }
        if l.state.connection.status == Some(ConnectionStatus::Connected) {
            for (name, m) in &l.state.modules {
                if m.intent_connect {
                    return Some(QueuedMsg {
                        method: "module".into(),
                        payload: Some(serde_json::json!({"name": name})),
                    });
                }
            }
        }
        None
    }

    // --- Account operations ---

    pub fn handle_account(
        &mut self,
        method: &str,
        payload: &Value,
    ) -> Result<(Value, Vec<String>), LinkError> {
        match method {
            "set_invite_mode" => {
                let mode_str = payload
                    .get("mode")
                    .and_then(Value::as_str)
                    .ok_or(LinkError::BadPayload)?;
                let mode = match mode_str {
                    "closed" => InviteMode::Closed,
                    "invite" => InviteMode::Invite,
                    "open" => InviteMode::Open,
                    _ => return Err(LinkError::InvalidMode(mode_str.to_string())),
                };
                self.invite_mode = mode;
                Ok((serde_json::json!("ok"), vec![]))
            }
            "set_modules" => {
                let peer = payload
                    .get("peer")
                    .and_then(Value::as_str)
                    .ok_or(LinkError::BadPayload)?;
                let modules: Vec<String> = payload
                    .get("modules")
                    .and_then(Value::as_array)
                    .ok_or(LinkError::BadPayload)?
                    .iter()
                    .filter_map(Value::as_str)
                    .map(String::from)
                    .collect();
                let supported = self.supported.clone();
                let l = self
                    .links
                    .get_mut(peer)
                    .ok_or_else(|| LinkError::NoPeer(peer.to_string()))?;
                l.set_modules(&modules, &supported);
                Ok((serde_json::json!("ok"), vec![peer.to_string()]))
            }
            "accept_module" => {
                let peer = payload
                    .get("peer")
                    .and_then(Value::as_str)
                    .ok_or(LinkError::BadPayload)?;
                let module = payload
                    .get("module")
                    .and_then(Value::as_str)
                    .ok_or(LinkError::BadPayload)?;
                let l = self
                    .links
                    .get_mut(peer)
                    .ok_or_else(|| LinkError::NoPeer(peer.to_string()))?;
                l.accept_module(module)?;
                Ok((serde_json::json!("ok"), vec![peer.to_string()]))
            }
            "reject_module" => {
                let peer = payload
                    .get("peer")
                    .and_then(Value::as_str)
                    .ok_or(LinkError::BadPayload)?;
                let module = payload
                    .get("module")
                    .and_then(Value::as_str)
                    .ok_or(LinkError::BadPayload)?;
                let l = self
                    .links
                    .get_mut(peer)
                    .ok_or_else(|| LinkError::NoPeer(peer.to_string()))?;
                l.reject_module(module)?;
                Ok((serde_json::json!("ok"), vec![]))
            }
            "status" => Ok((self.status(), vec![])),
            _ => Err(LinkError::UnknownMethod(method.to_string())),
        }
    }

    pub fn status(&self) -> Value {
        let mut links = serde_json::Map::new();
        for (peer, l) in &self.links {
            links.insert(
                peer.clone(),
                serde_json::json!({"state": l.state}),
            );
        }
        serde_json::json!({
            "links": links,
            "invite_mode": self.invite_mode,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn turn_bit_ordering() {
        let l = Link::new("alice", "bob");
        assert_eq!(l.turn_bit, 0);
        let l = Link::new("bob", "alice");
        assert_eq!(l.turn_bit, 1);
    }

    #[test]
    fn validate_request_normal() {
        let l = Link::new("alice", "bob");
        assert!(l.validate_request(0).unwrap().is_none());
    }

    #[test]
    fn validate_request_stale() {
        let mut l = Link::new("alice", "bob");
        l.counter = 5;
        assert!(matches!(
            l.validate_request(3),
            Err(LinkError::StaleCounter)
        ));
    }

    #[test]
    fn validate_request_ahead() {
        let l = Link::new("alice", "bob");
        assert!(matches!(
            l.validate_request(1),
            Err(LinkError::CounterAhead)
        ));
    }

    #[test]
    fn validate_request_cached() {
        let mut l = Link::new("alice", "bob");
        l.counter = 1;
        l.cached_response = Some(SyncResponse {
            counter: 0,
            ok: true,
            timestamp: 100,
        });
        let resp = l.validate_request(0).unwrap().unwrap();
        assert!(resp.ok);
        assert_eq!(resp.timestamp, 100);
    }

    #[test]
    fn accept_request_advances_counter() {
        let mut l = Link::new("alice", "bob");
        let resp = l.accept_request(0, true, 1000);
        assert!(resp.ok);
        assert_eq!(l.counter, 1);
        assert!(l.pending.is_none());
    }

    #[test]
    fn accept_response_advances_counter() {
        let mut l = Link::new("alice", "bob");
        l.counter = 1;
        l.pending = Some(QueuedMsg {
            method: "test".into(),
            payload: None,
        });
        l.accept_response(2000);
        assert_eq!(l.counter, 2);
        assert!(l.pending.is_none());
    }

    #[test]
    fn module_enabled() {
        let mut l = Link::new("a", "b");
        assert!(!l.module_enabled("loops"));
        l.state.modules.insert(
            "loops".into(),
            ConnectionState {
                status: Some(ConnectionStatus::Connected),
                ..Default::default()
            },
        );
        assert!(l.module_enabled("loops"));
    }

    #[test]
    fn ref_counting() {
        let mut c = ConnectionState::default();
        c.ref_inc();
        c.ref_inc();
        assert_eq!(c.refs, 2);
        c.unref();
        assert_eq!(c.refs, 1);
        c.unref_close();
        assert_eq!(c.refs, 0);
        assert!(c.intent_close);
    }

    #[test]
    fn connect_flow() {
        let supported = HashMap::new();
        let mut alice = Links::new("alice".into(), HashMap::new(), InviteMode::Invite, supported.clone());
        let mut bob = Links::new("bob".into(), HashMap::new(), InviteMode::Invite, supported);

        // Alice creates link with intent to connect.
        let la = alice.create("bob");
        la.state.connection.intent_connect = true;

        // Bob creates link with intent to connect.
        let lb = bob.create("alice");
        lb.state.connection.intent_connect = true;

        // Alice sends connect, Bob receives it — mutual intent.
        let r = bob
            .handle_request("alice", "connect", &Value::Null)
            .unwrap();
        assert!(r.ok);
        assert_eq!(
            bob.get("alice").unwrap().state.connection.status,
            Some(ConnectionStatus::Connected)
        );

        // Bob's response reaches Alice.
        alice.handle_response("bob", "connect", &Value::Null, true);
        assert_eq!(
            alice.get("bob").unwrap().state.connection.status,
            Some(ConnectionStatus::Connected)
        );
    }
}
