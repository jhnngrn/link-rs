//! Bilateral sync primitive — data structures, protocol types and sync loop
//! for bilateral links between parties.

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

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
    MaxLinks,
    InvitesDisabled,
    InvalidMode(String),
    RefsHeld,
    NoIncomingModule(String),
    UnknownMethod(String),
    Timestamp,
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
            Self::MaxLinks => write!(f, "max links"),
            Self::InvitesDisabled => write!(f, "invites disabled"),
            Self::InvalidMode(m) => write!(f, "invalid mode {m:?}"),
            Self::NoIncomingModule(m) => write!(f, "no incoming module {m}"),
            Self::UnknownMethod(m) => write!(f, "unknown link method {m:?}"),
            Self::Timestamp => write!(f, "timestamp out of range"),
            Self::Other(s) => write!(f, "{s}"),
        }
    }
}

impl std::error::Error for LinkError {}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum number of links per node.
pub const MAX_LINKS: usize = 64;

/// Maximum number of seconds a peer's timestamp may be ahead of our local
/// clock. Generous to tolerate clock drift; the main defence against
/// exploitation is the backward check (monotonicity).
pub const MAX_TIMESTAMP_FUTURE: i64 = 600; // 10 minutes

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
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub refs: u32,
}

fn is_zero_u32(v: &u32) -> bool {
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
// Module trait
// ---------------------------------------------------------------------------

/// Module processes sync methods and account API calls for a namespace.
/// HandleRequest returns (ok, kicks, err) where kicks is a list of peers
/// that need kicking on other links (cross-link side effects).
/// HandleResponse returns kicks for the same purpose.
/// HandleAccount handles user-facing API calls.
/// NextTx returns the next queued message to send on a peer, or None.
pub trait Module: Send + Sync {
    fn name(&self) -> &str;
    fn handle_request(
        &mut self,
        peer: &str,
        method: &str,
        payload: &RawValue,
        ts: i64,
    ) -> Result<(bool, Vec<String>), LinkError>;
    fn handle_response(
        &mut self,
        peer: &str,
        method: &str,
        payload: &RawValue,
        ok: bool,
        ts: i64,
    ) -> Vec<String>;
    fn handle_account(
        &mut self,
        method: &str,
        payload: &RawValue,
    ) -> Result<(Box<RawValue>, Vec<String>), LinkError>;
    /// Returns (method, payload, queued_at). None means nothing to send.
    fn next_tx(&self, peer: &str) -> Option<(String, Box<RawValue>, i64)>;
}

// ---------------------------------------------------------------------------
// QueuedMsg
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedMsg {
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Box<RawValue>>,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub queued_at: i64,
}

fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}

fn null_raw() -> Box<RawValue> {
    RawValue::from_string("null".into()).unwrap()
}

fn to_raw(val: &impl Serialize) -> Box<RawValue> {
    serde_json::value::to_raw_value(val).unwrap()
}

/// Reports whether a should win over b (older queued_at wins).
/// Zero queued_at means unset — unset loses to any set value.
/// Equal values (including both zero) return false, preserving
/// the earlier candidate.
pub fn older_queued_msg(a: &QueuedMsg, b: Option<&QueuedMsg>) -> bool {
    let Some(b) = b else {
        return true;
    };
    if a.queued_at == 0 && b.queued_at == 0 {
        return false;
    }
    if a.queued_at == 0 {
        return false;
    }
    if b.queued_at == 0 {
        return true;
    }
    a.queued_at < b.queued_at
}

// ---------------------------------------------------------------------------
// PeerSend — outgoing send lifecycle tracking
// ---------------------------------------------------------------------------

/// PeerSend tracks the outgoing send lifecycle for a link at a given counter.
/// InFlight means a thread is blocked on Send.
/// After the send completes, a retry timer is set: refractory after error
/// (blocks kicks), non-refractory after success (fallback, kicks bypass).
/// Both are scoped to counter — if counter advances via HandleSync,
/// the entry is stale and ignored.
#[derive(Debug)]
pub struct PeerSend {
    pub counter: u64,
    pub in_flight: bool,
    pub refractory: bool,
    // Timer handle — when set, a thread is sleeping and will call sendPending.
    // We track it as an Arc<()> "cancel token": when we replace PeerSend,
    // the old Arc drops and the timer thread detects it via weak ref.
    pub cancel: Option<Arc<()>>,
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
    pub payload: Option<Box<RawValue>>,
    #[serde(default)]
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

#[derive(Debug, Serialize, Deserialize)]
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
    #[serde(skip)]
    pub send: Option<PeerSend>,
}

impl Clone for Link {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            counter: self.counter,
            turn_bit: self.turn_bit,
            pending: self.pending.clone(),
            cached_response: self.cached_response.clone(),
            timestamp: self.timestamp,
            send: None, // runtime state, not cloned
        }
    }
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
            send: None,
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

    // --- Sync lifecycle ---

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

    /// Validate that ts is within acceptable bounds:
    /// - not before the link's latest timestamp (monotonicity)
    /// - not more than MAX_TIMESTAMP_FUTURE seconds ahead of now
    pub fn validate_timestamp(&self, ts: i64, now: i64) -> Result<(), LinkError> {
        if ts < self.timestamp {
            return Err(LinkError::Timestamp);
        }
        if ts > now + MAX_TIMESTAMP_FUTURE {
            return Err(LinkError::Timestamp);
        }
        Ok(())
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

    // --- Module negotiation ---

    fn req_module(
        &mut self,
        payload: &Value,
        modules: &HashMap<String, Box<dyn Module>>,
        invite_mode: InviteMode,
    ) -> Result<bool, LinkError> {
        let name = payload
            .get("name")
            .and_then(Value::as_str)
            .ok_or(LinkError::BadPayload)?;
        if name.is_empty() {
            return Err(LinkError::BadPayload);
        }
        if !modules.contains_key(name) {
            return Ok(false); // we don't speak this module
        }

        // Already connected — idempotent ACK.
        if let Some(m) = self.state.modules.get(name) {
            if m.status == Some(ConnectionStatus::Connected) {
                return Ok(true);
            }
        }

        // Mutual — we also want this module.
        if let Some(m) = self.state.modules.get(name) {
            if m.intent_connect || m.status == Some(ConnectionStatus::Waiting) {
                let m = self.state.modules.get_mut(name).unwrap();
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
                .entry(name.to_string())
                .or_insert_with(ConnectionState::default);
            m.status = Some(ConnectionStatus::Connected);
            return Ok(true);
        }

        // Mark as incoming — awaiting local accept/reject.
        let m = self
            .state
            .modules
            .entry(name.to_string())
            .or_insert_with(ConnectionState::default);
        m.status = Some(ConnectionStatus::Incoming);
        Ok(false) // NACK
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
    /// Modules not in the desired set and not yet connected are removed,
    /// unless they are in the protected set.
    pub fn set_modules(
        &mut self,
        modules: &[String],
        registered: &HashMap<String, Box<dyn Module>>,
        protected: &HashMap<String, ()>,
    ) {
        let desired: HashMap<&str, ()> = modules
            .iter()
            .filter(|n| registered.contains_key(n.as_str()))
            .map(|n| (n.as_str(), ()))
            .collect();

        for &name in desired.keys() {
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
                !protected.contains_key(name.as_str())
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
// EngineConfig — callbacks the sync loop needs
// ---------------------------------------------------------------------------

/// EngineConfig provides the callbacks Links needs to run the sync loop.
pub struct EngineConfig {
    /// Send delivers a sync message to a peer and returns the response body.
    pub send: Box<dyn Fn(&str, &SyncRequest) -> Result<Vec<u8>, String> + Send + Sync>,
    /// Commit persists the current state. Called under the mutex.
    pub commit: Box<dyn Fn() + Send + Sync>,
    /// Now returns the current time as unix seconds.
    pub now: Box<dyn Fn() -> i64 + Send + Sync>,
    /// Retry delay between retry attempts.
    pub retry_delay: Duration,
}

// ---------------------------------------------------------------------------
// Links — collection of links for a node, including the sync loop
// ---------------------------------------------------------------------------

/// Inner state protected by the mutex.
struct LinksInner {
    m: HashMap<String, Link>,
    self_id: String,
    invite_mode: InviteMode,
    protected: HashMap<String, ()>,
    modules: HashMap<String, Box<dyn Module>>,
    stopped: bool,
}

/// Links manages the set of bilateral links for a node.
/// Thread-safe — all methods acquire the internal mutex.
pub struct Links {
    inner: Arc<Mutex<LinksInner>>,
    engine: Arc<EngineConfig>,
}

impl Links {
    pub fn new(
        self_id: String,
        m: HashMap<String, Link>,
        mode: InviteMode,
        engine: EngineConfig,
    ) -> Self {
        let retry_delay = if engine.retry_delay == Duration::ZERO {
            Duration::from_secs(60)
        } else {
            engine.retry_delay
        };
        let engine = Arc::new(EngineConfig {
            send: engine.send,
            commit: engine.commit,
            now: engine.now,
            retry_delay,
        });
        Self {
            inner: Arc::new(Mutex::new(LinksInner {
                m,
                self_id,
                invite_mode: mode,
                protected: HashMap::new(),
                modules: HashMap::new(),
                stopped: false,
            })),
            engine,
        }
    }

    /// Returns the node identity.
    pub fn self_id(&self) -> String {
        self.inner.lock().unwrap().self_id.clone()
    }

    /// Register a module.
    pub fn register_module(&self, m: Box<dyn Module>) {
        let name = m.name().to_string();
        self.inner.lock().unwrap().modules.insert(name, m);
    }

    /// Add a module name to the protected set (SetModules cannot remove it).
    pub fn protect_module(&self, name: &str) {
        self.inner
            .lock()
            .unwrap()
            .protected
            .insert(name.to_string(), ());
    }

    pub fn get_link(&self, peer: &str) -> Option<Link> {
        self.inner.lock().unwrap().m.get(peer).cloned()
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().m.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().m.is_empty()
    }

    /// Returns the invite mode.
    pub fn invite_mode(&self) -> InviteMode {
        self.inner.lock().unwrap().invite_mode
    }

    /// Set the invite mode.
    pub fn set_invite_mode(&self, mode: InviteMode) {
        self.inner.lock().unwrap().invite_mode = mode;
    }

    /// Returns a snapshot of all links.
    pub fn map(&self) -> HashMap<String, Link> {
        self.inner.lock().unwrap().m.clone()
    }

    /// Create a new link to peer.
    pub fn create(&self, peer: &str) -> Link {
        let mut inner = self.inner.lock().unwrap();
        let lk = Link::new(&inner.self_id, peer);
        inner.m.entry(peer.to_string()).or_insert(lk).clone()
    }

    /// Delete a link.
    pub fn delete(&self, peer: &str) {
        self.inner.lock().unwrap().m.remove(peer);
    }

    /// Returns true if at max links.
    pub fn full(&self) -> bool {
        self.inner.lock().unwrap().m.len() >= MAX_LINKS
    }

    // --- Sync loop: incoming ---

    /// QuickReject returns true if this request can be rejected before
    /// expensive signature verification.
    pub fn quick_reject(&self, from: &str, method: &str) -> bool {
        let from = from.to_lowercase();
        let inner = self.inner.lock().unwrap();
        inner.m.get(&from).is_none() && method != "link.connect" && method != "link.close"
    }

    /// HandleSync processes an incoming sync request.
    pub fn handle_sync(&self, mut env: SyncRequest) -> Result<SyncResponse, LinkError> {
        env.from = env.from.to_lowercase();
        let mut inner = self.inner.lock().unwrap();

        if inner.m.get(&env.from).is_none() {
            if env.method == "link.close" {
                let now = (self.engine.now)();
                return Ok(SyncResponse {
                    counter: env.counter,
                    ok: true,
                    timestamp: now,
                });
            }
            if env.method != "link.connect" {
                return Err(LinkError::NoPeer(env.from.clone()));
            }
            if inner.invite_mode == InviteMode::Closed {
                return Err(LinkError::InvitesDisabled);
            }
            if inner.m.len() >= MAX_LINKS {
                return Err(LinkError::MaxLinks);
            }
            let lk = Link::new(&inner.self_id, &env.from);
            inner.m.insert(env.from.clone(), lk);
            inner.m.get_mut(&env.from).unwrap().state.connection.status =
                Some(ConnectionStatus::Incoming);
        }

        let now = (self.engine.now)();

        // Piggybacked response.
        let mut pb_kicks = Vec::new();
        if let Some(ref resp) = env.resp {
            let lk = inner.m.get(&env.from).unwrap();
            if lk.pending.is_some() && resp.counter == lk.counter {
                lk.validate_timestamp(resp.timestamp, now)?;
                let pending_method = lk.pending.as_ref().unwrap().method.clone();
                let pending_payload = lk.pending.as_ref().unwrap().payload.clone();
                let resp_ok = resp.ok;
                let resp_ts = resp.timestamp;
                let nr = null_raw();
                let payload_ref = pending_payload.as_deref().unwrap_or(&nr);
                pb_kicks = Self::dispatch_response_inner(
                    &mut inner,
                    &env.from,
                    &pending_method,
                    payload_ref,
                    resp_ok,
                    resp_ts,
                );
                let lk = inner.m.get_mut(&env.from).unwrap();
                lk.accept_response(resp_ts);
                (self.engine.commit)();
            }
        }

        // Validate counter and collision.
        let lk = inner.m.get(&env.from).unwrap();
        if let Some(cached) = lk.validate_request(env.counter)? {
            drop(inner);
            for k in pb_kicks {
                self.kick(&k);
            }
            return Ok(cached);
        }

        // Determine timestamp.
        let lk = inner.m.get(&env.from).unwrap();
        let ts = if lk.dominant() {
            let mut ts = now;
            if ts < lk.timestamp {
                ts = lk.timestamp;
            }
            ts
        } else {
            env.timestamp
        };

        // Validate peer timestamp.
        let lk = inner.m.get(&env.from).unwrap();
        lk.validate_timestamp(ts, now)?;

        // Dispatch to handler.
        let nr2 = null_raw();
        let req_payload = env.payload.as_deref().unwrap_or(&nr2);
        let (ok, kicks) =
            Self::dispatch_request_inner(&mut inner, &env.from, &env.method, req_payload, ts)?;

        let lk = inner.m.get_mut(&env.from).unwrap();
        let resp = lk.accept_request(env.counter, ok, ts);
        (self.engine.commit)();

        let from = env.from.clone();
        drop(inner);

        self.kick(&from);
        for k in pb_kicks {
            self.kick(&k);
        }
        for k in kicks {
            self.kick(&k);
        }

        Ok(resp)
    }

    // --- Sync loop: outgoing ---

    /// Kick triggers a sendPending for the given peer.
    pub fn kick(&self, peer: &str) {
        let peer = peer.to_string();
        let links = Links {
            inner: Arc::clone(&self.inner),
            engine: Arc::clone(&self.engine),
        };
        thread::spawn(move || {
            links.send_pending(&peer);
        });
    }

    /// KickAll kicks all peers. Used at boot to resume pending work.
    pub fn kick_all(&self) {
        let peers: Vec<String> = self.inner.lock().unwrap().m.keys().cloned().collect();
        for p in peers {
            self.kick(&p);
        }
    }

    /// Schedule runs fn after delay under the Links mutex.
    /// fn returns a list of peers to kick. Links commits and kicks.
    pub fn schedule<F>(&self, delay: Duration, f: F)
    where
        F: FnOnce(&mut HashMap<String, Link>) -> Vec<String> + Send + 'static,
    {
        let inner = Arc::clone(&self.inner);
        let engine = Arc::clone(&self.engine);
        let links = Links {
            inner: Arc::clone(&self.inner),
            engine: Arc::clone(&self.engine),
        };
        thread::spawn(move || {
            thread::sleep(delay);
            let mut guard = inner.lock().unwrap();
            if guard.stopped {
                return;
            }
            let kicks = f(&mut guard.m);
            (engine.commit)();
            drop(guard);
            for k in kicks {
                links.kick(&k);
            }
        });
    }

    /// Stop marks the Links as stopped. After Stop, Schedule callbacks
    /// and sendPending are no-ops.
    pub fn stop(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.stopped = true;
        // Cancel all pending send timers.
        for (_, lk) in inner.m.iter_mut() {
            lk.send = None;
        }
    }

    fn send_pending(&self, peer: &str) {
        let mut inner = self.inner.lock().unwrap();
        if inner.stopped {
            return;
        }
        let Some(lk) = inner.m.get_mut(peer) else {
            return;
        };

        // Check send lifecycle.
        if let Some(ref s) = lk.send {
            if s.counter == lk.counter && (s.in_flight || s.refractory) {
                return;
            }
        }
        lk.send = None;

        // Promote if needed.
        let mut promoted = false;
        if lk.pending.is_none() {
            let msg = Self::next_tx_inner(&inner, peer);
            if let Some(msg) = msg {
                inner.m.get_mut(peer).unwrap().pending = Some(msg);
                promoted = true;
            } else {
                return;
            }
        }

        let lk = inner.m.get_mut(peer).unwrap();
        let counter = lk.counter;
        let is_dominant = lk.dominant();
        let floor = lk.timestamp;
        let msg = lk.pending.clone().unwrap();
        let resp_piggyback = lk.cached_response.clone();
        lk.send = Some(PeerSend {
            counter,
            in_flight: true,
            refractory: false,
            cancel: None,
        });

        let mut env = SyncRequest {
            from: inner.self_id.clone(),
            to: peer.to_string(),
            counter,
            method: msg.method.clone(),
            payload: msg.payload.clone(),
            timestamp: 0,
            resp: resp_piggyback,
        };
        if is_dominant {
            env.timestamp = (self.engine.now)();
            if env.timestamp < floor {
                env.timestamp = floor;
            }
        }
        if promoted {
            (self.engine.commit)();
        }
        let self_id = inner.self_id.clone();
        drop(inner);

        let result = (self.engine.send)(peer, &env);

        let mut inner = self.inner.lock().unwrap();
        let peer_s = peer.to_string();

        match result {
            Err(e) => {
                log::warn!("[link] {}->{} {}: {}", self_id, peer, msg.method, e);
                self.set_retry_inner(&mut inner, &peer_s, counter, true);
                return;
            }
            Ok(body) => {
                let resp: SyncResponse = match serde_json::from_slice(&body) {
                    Ok(r) => r,
                    Err(_) => {
                        self.set_retry_inner(&mut inner, &peer_s, counter, true);
                        return;
                    }
                };

                let Some(lk) = inner.m.get(peer) else {
                    return;
                };
                // Stale check.
                if lk.pending.is_none()
                    || lk.pending.as_ref().unwrap().method != msg.method
                    || lk.counter != counter
                    || resp.counter != counter
                {
                    if let Some(lk) = inner.m.get_mut(peer) {
                        lk.send = None;
                    }
                    return;
                }

                let now = (self.engine.now)();
                if lk.validate_timestamp(resp.timestamp, now).is_err() {
                    log::warn!(
                        "[link] {}->{}: bad response timestamp",
                        self_id, peer
                    );
                    self.set_retry_inner(&mut inner, &peer_s, counter, true);
                    return;
                }

                let nr = null_raw();
                let payload_ref = msg.payload.as_deref().unwrap_or(&nr);
                let extra_kicks = Self::dispatch_response_inner(
                    &mut inner,
                    peer,
                    &msg.method,
                    payload_ref,
                    resp.ok,
                    resp.timestamp,
                );

                let lk = inner.m.get_mut(peer).unwrap();
                lk.accept_response(resp.timestamp);

                let kick_peer = if inner.m.contains_key(peer) {
                    let next = Self::next_tx_inner(&inner, peer);
                    if let Some(next) = next {
                        inner.m.get_mut(peer).unwrap().pending = Some(next);
                        true
                    } else {
                        false
                    }
                } else {
                    false
                };

                (self.engine.commit)();
                if let Some(lk) = inner.m.get_mut(peer) {
                    lk.send = None;
                }
                drop(inner);

                if kick_peer {
                    self.kick(peer);
                }
                for k in extra_kicks {
                    self.kick(&k);
                }
            }
        }
    }

    fn set_retry_inner(
        &self,
        inner: &mut LinksInner,
        peer: &str,
        counter: u64,
        refractory: bool,
    ) {
        if inner.stopped {
            return;
        }
        let Some(lk) = inner.m.get_mut(peer) else {
            return;
        };
        let cancel = Arc::new(());
        let weak = Arc::downgrade(&cancel);
        lk.send = Some(PeerSend {
            counter,
            in_flight: false,
            refractory,
            cancel: Some(cancel),
        });

        let peer_s = peer.to_string();
        let links = Links {
            inner: Arc::clone(&self.inner),
            engine: Arc::clone(&self.engine),
        };
        let delay = self.engine.retry_delay;
        thread::spawn(move || {
            thread::sleep(delay);
            // Check if this timer is still the active one.
            if weak.upgrade().is_none() {
                return; // cancelled
            }
            {
                let mut inner = links.inner.lock().unwrap();
                if let Some(lk) = inner.m.get_mut(&peer_s) {
                    // Clear send only if it's still ours.
                    if let Some(ref s) = lk.send {
                        if s.counter == counter {
                            lk.send = None;
                        }
                    }
                }
            }
            links.send_pending(&peer_s);
        });
    }

    // --- Dispatch helpers (called with lock held) ---

    fn dispatch_request_inner(
        inner: &mut LinksInner,
        peer: &str,
        full_method: &str,
        payload: &RawValue,
        ts: i64,
    ) -> Result<(bool, Vec<String>), LinkError> {
        let (ns, method) = split_method(full_method);

        if ns == "link" {
            let lk = inner
                .m
                .get_mut(peer)
                .ok_or_else(|| LinkError::NoPeer(peer.to_string()))?;
            match method {
                "connect" => {
                    if lk.state.connection.intent_connect
                        || lk.state.connection.status == Some(ConnectionStatus::Waiting)
                    {
                        lk.state.connection.status = Some(ConnectionStatus::Connected);
                        lk.state.connection.intent_connect = false;
                        return Ok((true, vec![]));
                    }
                    if inner.invite_mode == InviteMode::Open {
                        lk.state.connection.status = Some(ConnectionStatus::Connected);
                        return Ok((true, vec![]));
                    }
                    Ok((false, vec![]))
                }
                "close" => {
                    if lk.state.connection.refs > 0 {
                        return Err(LinkError::RefsHeld);
                    }
                    inner.m.remove(peer);
                    Ok((true, vec![]))
                }
                "module" => {
                    let pv: Value = serde_json::from_str(payload.get())
                        .map_err(|_| LinkError::BadPayload)?;
                    let invite_mode = inner.invite_mode;
                    let lk = inner.m.get_mut(peer).unwrap();
                    let ok = lk.req_module(&pv, &inner.modules, invite_mode)?;
                    Ok((ok, vec![]))
                }
                _ => Err(LinkError::UnknownMethod(method.to_string())),
            }
        } else {
            let lk = inner
                .m
                .get(peer)
                .ok_or_else(|| LinkError::NoPeer(peer.to_string()))?;
            if !lk.module_enabled(ns) {
                return Err(LinkError::UnknownMethod(full_method.to_string()));
            }
            let module = inner
                .modules
                .get_mut(ns)
                .ok_or_else(|| LinkError::UnknownMethod(full_method.to_string()))?;
            module.handle_request(peer, method, payload, ts)
        }
    }

    fn dispatch_response_inner(
        inner: &mut LinksInner,
        peer: &str,
        full_method: &str,
        payload: &RawValue,
        ok: bool,
        ts: i64,
    ) -> Vec<String> {
        let (ns, method) = split_method(full_method);

        if ns == "link" {
            let Some(lk) = inner.m.get_mut(peer) else {
                return vec![];
            };
            match method {
                "connect" => {
                    lk.state.connection.status = if ok {
                        Some(ConnectionStatus::Connected)
                    } else {
                        Some(ConnectionStatus::Waiting)
                    };
                    lk.state.connection.intent_connect = false;
                }
                "close" => {
                    if ok {
                        inner.m.remove(peer);
                    } else if let Some(lk) = inner.m.get_mut(peer) {
                        lk.state.connection.intent_close = false;
                    }
                }
                "module" => {
                    if let Ok(pv) = serde_json::from_str::<Value>(payload.get()) {
                        lk.resp_module(&pv, ok);
                    }
                }
                _ => {}
            }
            vec![]
        } else {
            let Some(module) = inner.modules.get_mut(ns) else {
                return vec![];
            };
            module.handle_response(peer, method, payload, ok, ts)
        }
    }

    fn next_tx_inner(inner: &LinksInner, peer: &str) -> Option<QueuedMsg> {
        let lk = inner.m.get(peer)?;

        // Link-level messages have absolute priority.
        if lk.state.connection.intent_close && lk.state.connection.refs == 0 {
            return Some(QueuedMsg {
                method: "link.close".into(),
                payload: None,
                queued_at: 0,
            });
        }
        if lk.state.connection.intent_connect {
            return Some(QueuedMsg {
                method: "link.connect".into(),
                payload: None,
                queued_at: 0,
            });
        }
        if lk.state.connection.status == Some(ConnectionStatus::Connected) {
            for (name, m) in &lk.state.modules {
                if m.intent_connect {
                    return Some(QueuedMsg {
                        method: "link.module".into(),
                        payload: Some(to_raw(&serde_json::json!({"name": name}))),
                        queued_at: 0,
                    });
                }
            }
        }

        // Module messages — oldest QueuedAt wins.
        if lk.state.connection.status != Some(ConnectionStatus::Connected) {
            return None;
        }
        let mut best: Option<QueuedMsg> = None;
        for (name, module) in &inner.modules {
            if !lk.module_enabled(name) {
                continue;
            }
            if let Some((method, payload, queued_at)) = module.next_tx(peer) {
                let msg = QueuedMsg {
                    method: format!("{}.{}", name, method),
                    payload: if payload.get() == "null" { None } else { Some(payload) },
                    queued_at,
                };
                if older_queued_msg(&msg, best.as_ref()) {
                    best = Some(msg);
                }
            }
        }
        best
    }

    // --- Account operations ---

    /// HandleAccount dispatches an account API call by namespace.
    /// Handles locking, commit, and kicks internally.
    pub fn handle_account(
        &self,
        full_method: &str,
        payload: &RawValue,
    ) -> Result<Box<RawValue>, LinkError> {
        let mut inner = self.inner.lock().unwrap();
        let (ns, method) = split_method(full_method);

        let (result, kicks) = if ns == "link" {
            Self::handle_link_account_inner(&mut inner, method, payload)?
        } else {
            let module = inner
                .modules
                .get_mut(ns)
                .ok_or_else(|| LinkError::UnknownMethod(full_method.to_string()))?;
            module.handle_account(method, payload)?
        };

        (self.engine.commit)();
        drop(inner);
        for k in kicks {
            self.kick(&k);
        }
        Ok(result)
    }

    fn handle_link_account_inner(
        inner: &mut LinksInner,
        method: &str,
        payload: &RawValue,
    ) -> Result<(Box<RawValue>, Vec<String>), LinkError> {
        let pv: Value =
            serde_json::from_str(payload.get()).map_err(|_| LinkError::BadPayload)?;
        match method {
            "set_invite_mode" => {
                let mode_str = pv
                    .get("mode")
                    .and_then(Value::as_str)
                    .ok_or(LinkError::BadPayload)?;
                let mode = match mode_str {
                    "closed" => InviteMode::Closed,
                    "invite" => InviteMode::Invite,
                    "open" => InviteMode::Open,
                    _ => return Err(LinkError::InvalidMode(mode_str.to_string())),
                };
                inner.invite_mode = mode;
                Ok((to_raw(&"ok"), vec![]))
            }
            "set_modules" => {
                let peer = pv
                    .get("peer")
                    .and_then(Value::as_str)
                    .ok_or(LinkError::BadPayload)?;
                let modules: Vec<String> = pv
                    .get("modules")
                    .and_then(Value::as_array)
                    .ok_or(LinkError::BadPayload)?
                    .iter()
                    .filter_map(Value::as_str)
                    .map(String::from)
                    .collect();
                let lk = inner
                    .m
                    .get_mut(peer)
                    .ok_or_else(|| LinkError::NoPeer(peer.to_string()))?;
                lk.set_modules(&modules, &inner.modules, &inner.protected);
                Ok((to_raw(&"ok"), vec![peer.to_string()]))
            }
            "accept_module" => {
                let peer = pv
                    .get("peer")
                    .and_then(Value::as_str)
                    .ok_or(LinkError::BadPayload)?;
                let module = pv
                    .get("module")
                    .and_then(Value::as_str)
                    .ok_or(LinkError::BadPayload)?;
                let lk = inner
                    .m
                    .get_mut(peer)
                    .ok_or_else(|| LinkError::NoPeer(peer.to_string()))?;
                lk.accept_module(module)?;
                Ok((to_raw(&"ok"), vec![peer.to_string()]))
            }
            "reject_module" => {
                let peer = pv
                    .get("peer")
                    .and_then(Value::as_str)
                    .ok_or(LinkError::BadPayload)?;
                let module = pv
                    .get("module")
                    .and_then(Value::as_str)
                    .ok_or(LinkError::BadPayload)?;
                let lk = inner
                    .m
                    .get_mut(peer)
                    .ok_or_else(|| LinkError::NoPeer(peer.to_string()))?;
                lk.reject_module(module)?;
                Ok((to_raw(&"ok"), vec![]))
            }
            "status" => {
                let status = Self::status_inner(inner);
                Ok((status, vec![]))
            }
            _ => Err(LinkError::UnknownMethod(method.to_string())),
        }
    }

    fn status_inner(inner: &LinksInner) -> Box<RawValue> {
        let mut links = serde_json::Map::new();
        for (peer, l) in &inner.m {
            links.insert(
                peer.clone(),
                serde_json::json!({"state": l.state}),
            );
        }
        to_raw(&serde_json::json!({
            "links": links,
            "invite_mode": inner.invite_mode,
        }))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn split_method(method: &str) -> (&str, &str) {
    if let Some(i) = method.find('.') {
        (&method[..i], &method[i + 1..])
    } else {
        ("", method)
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
        assert_eq!(l.timestamp, 1000);
    }

    #[test]
    fn accept_response_advances_counter() {
        let mut l = Link::new("alice", "bob");
        l.counter = 1;
        l.pending = Some(QueuedMsg {
            method: "test".into(),
            payload: None,
            queued_at: 0,
        });
        l.accept_response(2000);
        assert_eq!(l.counter, 2);
        assert!(l.pending.is_none());
        assert_eq!(l.timestamp, 2000);
    }

    #[test]
    fn validate_timestamp_ok() {
        let mut l = Link::new("alice", "bob");
        l.timestamp = 100;
        assert!(l.validate_timestamp(100, 200).is_ok());
        assert!(l.validate_timestamp(150, 200).is_ok());
    }

    #[test]
    fn validate_timestamp_below_floor() {
        let mut l = Link::new("alice", "bob");
        l.timestamp = 100;
        assert!(matches!(
            l.validate_timestamp(99, 200),
            Err(LinkError::Timestamp)
        ));
    }

    #[test]
    fn validate_timestamp_too_far_future() {
        let l = Link::new("alice", "bob");
        assert!(matches!(
            l.validate_timestamp(1000, 200),
            Err(LinkError::Timestamp)
        ));
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
    fn older_queued_msg_basic() {
        let a = QueuedMsg { method: "a".into(), payload: None, queued_at: 10 };
        let b = QueuedMsg { method: "b".into(), payload: None, queued_at: 20 };
        assert!(older_queued_msg(&a, Some(&b)));
        assert!(!older_queued_msg(&b, Some(&a)));
    }

    #[test]
    fn older_queued_msg_zero() {
        let a = QueuedMsg { method: "a".into(), payload: None, queued_at: 0 };
        let b = QueuedMsg { method: "b".into(), payload: None, queued_at: 10 };
        assert!(!older_queued_msg(&a, Some(&b))); // zero loses
        assert!(older_queued_msg(&b, Some(&a))); // set wins over zero
    }

    #[test]
    fn older_queued_msg_none() {
        let a = QueuedMsg { method: "a".into(), payload: None, queued_at: 0 };
        assert!(older_queued_msg(&a, None)); // anything wins over None
    }

    #[test]
    fn split_method_basic() {
        assert_eq!(split_method("link.connect"), ("link", "connect"));
        assert_eq!(split_method("trustline.payment"), ("trustline", "payment"));
        assert_eq!(split_method("close"), ("", "close"));
    }

    #[test]
    fn handle_sync_connect_flow() {
        use std::sync::atomic::{AtomicI64, Ordering};
        let clock = Arc::new(AtomicI64::new(1000));
        let alice = Links::new(
            "alice".into(),
            HashMap::new(),
            InviteMode::Invite,
            EngineConfig {
                send: Box::new(|_, _| Err("not wired".into())),
                commit: Box::new(|| {}),
                now: Box::new(move || clock.load(Ordering::SeqCst)),
                retry_delay: Duration::from_secs(60),
            },
        );

        // Bob connects to alice.
        let env = SyncRequest {
            from: "bob".into(),
            to: "alice".into(),
            counter: 0,
            method: "link.connect".into(),
            payload: None,
            timestamp: 1000,
            resp: None,
        };
        let resp = alice.handle_sync(env).unwrap();
        // Invite mode — NACK, link is incoming.
        assert!(!resp.ok);
        let lk = alice.get_link("bob").unwrap();
        assert_eq!(lk.state.connection.status, Some(ConnectionStatus::Incoming));
    }
}
