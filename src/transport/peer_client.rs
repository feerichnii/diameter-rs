//! Multi-peer Diameter client (Active/Active, topology A).
//!
//! One `DiameterPeerClient` holds several peer connections that may share a
//! single local `IP:port`. Each peer has its own reader task; Answers are
//! matched by Hop-by-Hop on that connection, and peer-initiated Requests are
//! delivered to the application via `take_incoming_requests`.

use crate::avp::flags::M;
use crate::avp::{Identity, Unsigned32};
use crate::diameter::{flags, CommandCode, DiameterMessage};
use crate::dictionary::Dictionary;
use crate::error::{Error, Result};
use crate::transport::connection::{
    connect_stream, resolve_address, validate_shared_local_addr, ConnectionConfig, SocketOpts,
};
use crate::transport::Codec;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout, Sleep};

/// How `send` chooses among connected peers for a new outgoing Request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PeerSelection {
    #[default]
    Failover,
    RoundRobin,
}

/// Client-wide settings for [`DiameterPeerClient`].
#[derive(Clone)]
pub struct PeerClientConfig {
    pub local_addr: Option<SocketAddr>,
    /// Origin-Host put into link-local answers the client generates itself
    /// (currently the automatic DWA to a peer's DWR).
    pub origin_host: Option<String>,
    /// Origin-Realm for the same link-local answers.
    pub origin_realm: Option<String>,
    pub selection: PeerSelection,
    pub request_timeout: Duration,
    pub incoming_request_timeout: Duration,
    pub max_pending: usize,
    pub max_inbound_pending: usize,
    pub reuse_addr: bool,
    pub reuse_port: bool,
}

impl Default for PeerClientConfig {
    fn default() -> Self {
        Self {
            local_addr: None,
            origin_host: None,
            origin_realm: None,
            selection: PeerSelection::Failover,
            request_timeout: Duration::from_secs(30),
            incoming_request_timeout: Duration::from_secs(30),
            max_pending: 1024,
            max_inbound_pending: 1024,
            reuse_addr: false,
            reuse_port: false,
        }
    }
}

impl PeerClientConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn local_addr(mut self, addr: SocketAddr) -> Self {
        self.local_addr = Some(addr);
        // Shared endpoint needs reuse; enable by default when local_addr is set.
        self.reuse_addr = true;
        self.reuse_port = true;
        self
    }

    pub fn origin_host(mut self, host: impl Into<String>) -> Self {
        self.origin_host = Some(host.into());
        self
    }

    pub fn origin_realm(mut self, realm: impl Into<String>) -> Self {
        self.origin_realm = Some(realm.into());
        self
    }

    pub fn selection(mut self, selection: PeerSelection) -> Self {
        self.selection = selection;
        self
    }

    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub fn incoming_request_timeout(mut self, timeout: Duration) -> Self {
        self.incoming_request_timeout = timeout;
        self
    }

    pub fn max_pending(mut self, max: usize) -> Self {
        self.max_pending = max;
        self
    }

    pub fn max_inbound_pending(mut self, max: usize) -> Self {
        self.max_inbound_pending = max;
        self
    }

    pub fn reuse_addr(mut self, enabled: bool) -> Self {
        self.reuse_addr = enabled;
        self
    }

    pub fn reuse_port(mut self, enabled: bool) -> Self {
        self.reuse_port = enabled;
        self
    }
}

/// Named peer entry: identity + connection settings.
#[derive(Clone)]
pub struct PeerConfig {
    pub name: String,
    pub conn: ConnectionConfig,
}

impl PeerConfig {
    pub fn new(name: impl Into<String>, address: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            conn: ConnectionConfig::new(address),
        }
    }

    pub fn connection(mut self, conn: ConnectionConfig) -> Self {
        self.conn = conn;
        self
    }
}

/// Result-Code for DIAMETER_SUCCESS.
const RC_SUCCESS: u32 = 2001;
/// Result-Code for DIAMETER_COMMAND_UNSUPPORTED.
const RC_COMMAND_UNSUPPORTED: u32 = 3001;
/// Result-Code for DIAMETER_UNABLE_TO_DELIVER.
const RC_UNABLE_TO_DELIVER: u32 = 3002;
/// Result-Code for DIAMETER_TOO_BUSY.
const RC_TOO_BUSY: u32 = 3004;

struct PendingEntry {
    sender: oneshot::Sender<DiameterMessage>,
}

struct PeerConnection {
    generation: u64,
    local_addr: SocketAddr,
    peer_addr: SocketAddr,
    writer: Arc<AsyncMutex<dyn AsyncWrite + Send + Unpin>>,
    pending: StdMutex<HashMap<u32, PendingEntry>>,
    connected: AtomicBool,
    peer_name: String,
    next_hbh: AtomicU64,
}

impl PeerConnection {
    fn mark_disconnected(&self) {
        if self
            .connected
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            if let Ok(mut pending) = self.pending.lock() {
                pending.clear();
            }
        }
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    async fn send_raw(&self, msg: &DiameterMessage) -> Result<()> {
        if !self.is_connected() {
            return Err(Error::ClientError(format!(
                "Peer {} is not connected",
                self.peer_name
            )));
        }
        let mut writer = self.writer.lock().await;
        match Codec::encode(&mut writer.deref_mut(), msg).await {
            Ok(()) => Ok(()),
            Err(e) => {
                self.mark_disconnected();
                Err(e)
            }
        }
    }
}

struct Peer {
    name: String,
    conn_config: ConnectionConfig,
    connect_lock: AsyncMutex<()>,
    current: StdMutex<Option<Arc<PeerConnection>>>,
    generation_counter: AtomicU64,
}

impl Peer {
    fn new(cfg: PeerConfig) -> Self {
        Self {
            name: cfg.name,
            conn_config: cfg.conn,
            connect_lock: AsyncMutex::new(()),
            current: StdMutex::new(None),
            generation_counter: AtomicU64::new(0),
        }
    }

    fn current_connection(&self) -> Option<Arc<PeerConnection>> {
        self.current.lock().ok().and_then(|g| g.clone())
    }

    fn is_connected(&self) -> bool {
        self.current_connection()
            .map(|c| c.is_connected())
            .unwrap_or(false)
    }

    fn take_current(&self) -> Option<Arc<PeerConnection>> {
        self.current.lock().ok().and_then(|mut g| g.take())
    }

    fn set_current(&self, conn: Option<Arc<PeerConnection>>) {
        if let Ok(mut g) = self.current.lock() {
            *g = conn;
        }
    }
}

struct PeerClientInner {
    config: PeerClientConfig,
    dict: Arc<Dictionary>,
    peers: StdMutex<Vec<Arc<Peer>>>,
    peer_by_name: StdMutex<HashMap<String, Arc<Peer>>>,
    /// Resolved remote addresses currently claimed by a peer (under this lock).
    remote_registry: StdMutex<HashMap<SocketAddr, String>>,
    rr_index: AtomicU64,
    inbound_tx: StdMutex<Option<mpsc::Sender<IncomingRequest>>>,
    inbound_rx: StdMutex<Option<mpsc::Receiver<IncomingRequest>>>,
    has_consumer: AtomicBool,
}

/// Multi-peer Diameter client.
#[derive(Clone)]
pub struct DiameterPeerClient {
    inner: Arc<PeerClientInner>,
}

/// Peer-initiated Request delivered to the application.
pub struct IncomingRequest {
    pub peer_name: String,
    pub message: DiameterMessage,
    connection: Arc<PeerConnection>,
    received_at: Instant,
    timeout: Duration,
}

impl IncomingRequest {
    /// Send an Answer on the **same** connection (generation) that carried the Request.
    ///
    /// Forces Hop-by-Hop / End-to-End from the request and clears the R flag.
    /// Does not allocate a new Hop-by-Hop (unlike `send` / `send_to`).
    pub async fn respond(self, mut answer: DiameterMessage) -> Result<()> {
        if self.received_at.elapsed() > self.timeout {
            return Err(Error::ClientError(format!(
                "Incoming request from peer {} expired before respond()",
                self.peer_name
            )));
        }
        if !self.connection.is_connected() {
            return Err(Error::ClientError(format!(
                "Peer {} connection is no longer active; answer not sent",
                self.peer_name
            )));
        }

        answer.set_hop_by_hop_id(self.message.get_hop_by_hop_id());
        answer.set_end_to_end_id(self.message.get_end_to_end_id());
        answer.set_flags(answer.get_flags() & !flags::REQUEST);

        self.connection.send_raw(&answer).await
    }

    pub fn hop_by_hop_id(&self) -> u32 {
        self.message.get_hop_by_hop_id()
    }

    pub fn end_to_end_id(&self) -> u32 {
        self.message.get_end_to_end_id()
    }
}

/// Future for an outgoing Request's Answer, with Drop-safe pending cleanup.
pub struct PeerResponseFuture {
    receiver: Option<oneshot::Receiver<DiameterMessage>>,
    connection: Arc<PeerConnection>,
    hop_by_hop: u32,
    deadline: Pin<Box<Sleep>>,
    finished: bool,
}

impl PeerResponseFuture {
    fn cleanup(&mut self) {
        if !self.finished {
            self.finished = true;
            if let Ok(mut pending) = self.connection.pending.lock() {
                pending.remove(&self.hop_by_hop);
            }
        }
    }
}

impl Drop for PeerResponseFuture {
    fn drop(&mut self) {
        self.cleanup();
    }
}

impl Future for PeerResponseFuture {
    type Output = Result<DiameterMessage>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.finished {
            return Poll::Ready(Err(Error::ClientError(
                "Response future already completed".into(),
            )));
        }

        if let Poll::Ready(()) = self.deadline.as_mut().poll(cx) {
            self.cleanup();
            return Poll::Ready(Err(Error::ClientError(format!(
                "Request timed out waiting for answer (hbh={})",
                self.hop_by_hop
            ))));
        }

        let receiver = match self.receiver.as_mut() {
            Some(rx) => rx,
            None => {
                return Poll::Ready(Err(Error::ClientError(
                    "Response channel missing".into(),
                )));
            }
        };

        match Pin::new(receiver).poll(cx) {
            Poll::Ready(Ok(msg)) => {
                self.finished = true;
                self.receiver.take();
                Poll::Ready(Ok(msg))
            }
            Poll::Ready(Err(_)) => {
                self.cleanup();
                self.receiver.take();
                Poll::Ready(Err(Error::ClientError(
                    "Response channel closed (peer disconnected)".into(),
                )))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl DiameterPeerClient {
    pub fn new(dict: Arc<Dictionary>, config: PeerClientConfig) -> Result<Self> {
        if let Some(local) = config.local_addr {
            validate_shared_local_addr(local)?;
        }

        let (tx, rx) = mpsc::channel(config.max_inbound_pending.max(1));
        let inner = PeerClientInner {
            config,
            dict,
            peers: StdMutex::new(Vec::new()),
            peer_by_name: StdMutex::new(HashMap::new()),
            remote_registry: StdMutex::new(HashMap::new()),
            rr_index: AtomicU64::new(0),
            inbound_tx: StdMutex::new(Some(tx)),
            inbound_rx: StdMutex::new(Some(rx)),
            has_consumer: AtomicBool::new(false),
        };
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    pub fn add_peer(&self, cfg: PeerConfig) -> Result<()> {
        if cfg.name.is_empty() {
            return Err(Error::ClientError("Peer name must not be empty".into()));
        }
        let mut by_name = self
            .inner
            .peer_by_name
            .lock()
            .map_err(|e| Error::LockError(e.to_string()))?;
        if by_name.contains_key(&cfg.name) {
            return Err(Error::ClientError(format!(
                "Peer '{}' already registered",
                cfg.name
            )));
        }
        let peer = Arc::new(Peer::new(cfg));
        by_name.insert(peer.name.clone(), Arc::clone(&peer));
        self.inner
            .peers
            .lock()
            .map_err(|e| Error::LockError(e.to_string()))?
            .push(peer);
        Ok(())
    }

    /// Take the single inbound Request receiver. Subsequent calls return `None`.
    pub fn take_incoming_requests(&self) -> Option<mpsc::Receiver<IncomingRequest>> {
        let rx = self.inner.inbound_rx.lock().ok()?.take()?;
        self.inner.has_consumer.store(true, Ordering::SeqCst);
        Some(rx)
    }

    /// Current approximate depth of the inbound Request queue (for metrics).
    pub fn inbound_pending(&self) -> usize {
        let max = self.inner.config.max_inbound_pending.max(1);
        let Some(tx) = self
            .inner
            .inbound_tx
            .lock()
            .ok()
            .and_then(|g| g.clone())
        else {
            return 0;
        };
        max.saturating_sub(tx.capacity())
    }

    pub fn is_connected(&self, peer_name: &str) -> bool {
        self.peer(peer_name)
            .map(|p| p.is_connected())
            .unwrap_or(false)
    }

    /// Local socket address of the active connection to `peer_name`, if connected.
    pub fn peer_local_addr(&self, peer_name: &str) -> Option<SocketAddr> {
        self.peer(peer_name)
            .ok()
            .and_then(|p| p.current_connection())
            .filter(|c| c.is_connected())
            .map(|c| c.local_addr)
    }

    /// Remote socket address of the active connection to `peer_name`, if connected.
    pub fn peer_remote_addr(&self, peer_name: &str) -> Option<SocketAddr> {
        self.peer(peer_name)
            .ok()
            .and_then(|p| p.current_connection())
            .filter(|c| c.is_connected())
            .map(|c| c.peer_addr)
    }

    /// Connection generation for `peer_name` (increments on each establish).
    pub fn peer_generation(&self, peer_name: &str) -> Option<u64> {
        self.peer(peer_name)
            .ok()
            .and_then(|p| p.current_connection())
            .map(|c| c.generation)
    }

    pub fn peer_names(&self) -> Vec<String> {
        self.inner
            .peers
            .lock()
            .map(|p| p.iter().map(|x| x.name.clone()).collect())
            .unwrap_or_default()
    }

    fn peer(&self, name: &str) -> Result<Arc<Peer>> {
        self.inner
            .peer_by_name
            .lock()
            .map_err(|e| Error::LockError(e.to_string()))?
            .get(name)
            .cloned()
            .ok_or_else(|| Error::ClientError(format!("Unknown peer '{}'", name)))
    }

    fn socket_opts(&self) -> SocketOpts {
        SocketOpts {
            reuse_addr: self.inner.config.reuse_addr,
            reuse_port: self.inner.config.reuse_port,
        }
    }

    /// Idempotent connect: if already connected, returns `Ok(())`.
    pub async fn connect(&self, peer_name: &str) -> Result<()> {
        let peer = self.peer(peer_name)?;
        let _guard = peer.connect_lock.lock().await;
        if peer.is_connected() {
            return Ok(());
        }
        self.establish_locked(&peer, false).await
    }

    /// Replace any existing connection with a new one.
    pub async fn reconnect(&self, peer_name: &str) -> Result<()> {
        let peer = self.peer(peer_name)?;
        let _guard = peer.connect_lock.lock().await;
        self.establish_locked(&peer, true).await
    }

    /// Connect all peers in parallel. Returns per-peer results.
    pub async fn connect_all(&self) -> Vec<(String, Result<()>)> {
        let names = self.peer_names();
        let mut handles = Vec::with_capacity(names.len());
        for name in names {
            let this = self.clone();
            handles.push(tokio::spawn(async move {
                let result = this.connect(&name).await;
                (name, result)
            }));
        }
        let mut out = Vec::with_capacity(handles.len());
        for h in handles {
            match h.await {
                Ok(pair) => out.push(pair),
                Err(e) => out.push((
                    String::new(),
                    Err(Error::ClientError(format!("connect task failed: {}", e))),
                )),
            }
        }
        out
    }

    /// Disconnect a peer and clear its pending Requests.
    pub async fn disconnect(&self, peer_name: &str) -> Result<()> {
        let peer = self.peer(peer_name)?;
        let _guard = peer.connect_lock.lock().await;
        self.teardown_locked(&peer);
        Ok(())
    }

    fn teardown_locked(&self, peer: &Peer) {
        if let Some(conn) = peer.take_current() {
            conn.mark_disconnected();
            // Drop writer by dropping Arc when reader exits / last refs go away.
        }
        // Free remote registry entry for this peer name.
        if let Ok(mut reg) = self.inner.remote_registry.lock() {
            reg.retain(|_, name| name != &peer.name);
        }
    }

    async fn establish_locked(&self, peer: &Arc<Peer>, force: bool) -> Result<()> {
        if !force && peer.is_connected() {
            return Ok(());
        }
        if force {
            self.teardown_locked(peer);
        }

        let remote = resolve_address(&peer.conn_config.address).await?;

        // Register unique remote under client lock (no network I/O under peer-table lock).
        {
            let mut reg = self
                .inner
                .remote_registry
                .lock()
                .map_err(|e| Error::LockError(e.to_string()))?;
            if let Some(owner) = reg.get(&remote) {
                if owner != &peer.name {
                    return Err(Error::ClientError(format!(
                        "Remote {} already claimed by peer '{}'",
                        remote, owner
                    )));
                }
            }
            reg.insert(remote, peer.name.clone());
        }

        let connected = match connect_stream(
            &peer.conn_config,
            self.inner.config.local_addr,
            self.socket_opts(),
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                // Unregister on failure if we still own the slot and have no connection.
                if let Ok(mut reg) = self.inner.remote_registry.lock() {
                    if reg.get(&remote).map(|n| n == &peer.name).unwrap_or(false)
                        && !peer.is_connected()
                    {
                        reg.remove(&remote);
                    }
                }
                return Err(e);
            }
        };

        if let Some(expected) = self.inner.config.local_addr {
            if connected.local_addr != expected {
                if let Ok(mut reg) = self.inner.remote_registry.lock() {
                    reg.remove(&remote);
                }
                return Err(Error::ClientError(format!(
                    "Connected local_addr {} does not match configured {}",
                    connected.local_addr, expected
                )));
            }
        }

        let generation = peer.generation_counter.fetch_add(1, Ordering::SeqCst) + 1;
        let writer: Arc<AsyncMutex<dyn AsyncWrite + Send + Unpin>> =
            Arc::new(AsyncMutex::new(connected.writer));
        let conn = Arc::new(PeerConnection {
            generation,
            local_addr: connected.local_addr,
            peer_addr: connected.peer_addr,
            writer,
            pending: StdMutex::new(HashMap::new()),
            connected: AtomicBool::new(true),
            peer_name: peer.name.clone(),
            next_hbh: AtomicU64::new(1),
        });

        // Publish connection then spawn reader — last steps for cancellation safety.
        peer.set_current(Some(Arc::clone(&conn)));

        let reader = connected.reader;
        let client = self.clone();
        let peer_name = peer.name.clone();
        let conn_for_reader = Arc::clone(&conn);
        let dict = Arc::clone(&self.inner.dict);
        let _handle: JoinHandle<()> = tokio::spawn(async move {
            client
                .reader_loop(peer_name, conn_for_reader, reader, dict)
                .await;
        });

        Ok(())
    }

    async fn reader_loop(
        &self,
        peer_name: String,
        conn: Arc<PeerConnection>,
        mut reader: Box<dyn AsyncRead + Send + Unpin>,
        dict: Arc<Dictionary>,
    ) {
        loop {
            match Codec::decode(&mut reader, Arc::clone(&dict)).await {
                Ok(msg) => {
                    if msg.get_flags() & flags::REQUEST != 0 {
                        self.dispatch_incoming_request(&peer_name, &conn, msg, &dict)
                            .await;
                    } else {
                        Self::dispatch_answer(&conn, msg);
                    }
                }
                Err(e) => {
                    match &e {
                        Error::IoError(ioe) if ioe.kind() == std::io::ErrorKind::UnexpectedEof => {
                            log::info!("[{}] Peer connection closed", peer_name);
                        }
                        _ => {
                            log::error!("[{}] Read error: {:?}", peer_name, e);
                        }
                    }
                    conn.mark_disconnected();
                    return;
                }
            }
        }
    }

    fn dispatch_answer(conn: &PeerConnection, msg: DiameterMessage) {
        let hbh = msg.get_hop_by_hop_id();
        let sender = {
            let mut pending = match conn.pending.lock() {
                Ok(p) => p,
                Err(_) => return,
            };
            pending.remove(&hbh)
        };
        match sender {
            Some(entry) => {
                let _ = entry.sender.send(msg);
            }
            None => {
                log::warn!(
                    "[{}] Dropping unexpected Answer hbh={} (no pending request)",
                    conn.peer_name,
                    hbh
                );
            }
        }
    }

    async fn dispatch_incoming_request(
        &self,
        peer_name: &str,
        conn: &Arc<PeerConnection>,
        msg: DiameterMessage,
        dict: &Arc<Dictionary>,
    ) {
        // Device-Watchdog is link-local by design (RFC 6733 §5.5): the reader
        // answers it itself, strictly on the same connection, and it never
        // reaches the application's inbound queue.
        if msg.get_command_code() == CommandCode::DeviceWatchdog {
            log::debug!("[{}] DWR received; answering DWA on the same socket", peer_name);
            let mut answer = make_base_answer(&msg, RC_SUCCESS, Arc::clone(dict));
            if let Some(host) = &self.inner.config.origin_host {
                answer.add_avp(264, None, M, Identity::new(host).into());
            }
            if let Some(realm) = &self.inner.config.origin_realm {
                answer.add_avp(296, None, M, Identity::new(realm).into());
            }
            let conn = Arc::clone(conn);
            let peer = peer_name.to_string();
            tokio::spawn(async move {
                if let Err(e) = conn.send_raw(&answer).await {
                    log::warn!("[{}] Failed to send DWA: {:?}", peer, e);
                }
            });
            return;
        }

        if !self.inner.has_consumer.load(Ordering::SeqCst) {
            log::warn!(
                "[{}] No inbound consumer registered; sending error Answer for cmd {:?}",
                peer_name,
                msg.get_command_code()
            );
            let answer = make_base_answer(&msg, RC_UNABLE_TO_DELIVER, Arc::clone(dict));
            let conn = Arc::clone(conn);
            tokio::spawn(async move {
                let _ = conn.send_raw(&answer).await;
            });
            return;
        }

        let incoming = IncomingRequest {
            peer_name: peer_name.to_string(),
            message: msg,
            connection: Arc::clone(conn),
            received_at: Instant::now(),
            timeout: self.inner.config.incoming_request_timeout,
        };

        let tx = {
            let guard = self.inner.inbound_tx.lock().ok();
            guard.and_then(|g| g.clone())
        };

        let Some(tx) = tx else {
            let answer = make_base_answer(
                &incoming.message,
                RC_COMMAND_UNSUPPORTED,
                Arc::clone(dict),
            );
            let conn = Arc::clone(conn);
            tokio::spawn(async move {
                let _ = conn.send_raw(&answer).await;
            });
            return;
        };

        match tx.try_send(incoming) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(incoming)) => {
                log::warn!(
                    "[{}] Inbound queue full; sending DIAMETER_TOO_BUSY",
                    peer_name
                );
                let answer = make_base_answer(&incoming.message, RC_TOO_BUSY, Arc::clone(dict));
                let conn = Arc::clone(conn);
                tokio::spawn(async move {
                    let _ = conn.send_raw(&answer).await;
                });
            }
            Err(mpsc::error::TrySendError::Closed(incoming)) => {
                log::warn!("[{}] Inbound channel closed", peer_name);
                let answer =
                    make_base_answer(&incoming.message, RC_UNABLE_TO_DELIVER, Arc::clone(dict));
                let conn = Arc::clone(conn);
                tokio::spawn(async move {
                    let _ = conn.send_raw(&answer).await;
                });
            }
        }
    }

    /// Send a Request to a specific peer.
    pub async fn send_to(
        &self,
        peer_name: &str,
        req: DiameterMessage,
    ) -> Result<PeerResponseFuture> {
        let peer = self.peer(peer_name)?;
        let conn = peer
            .current_connection()
            .filter(|c| c.is_connected())
            .ok_or_else(|| Error::ClientError(format!("Peer '{}' is not connected", peer_name)))?;
        self.send_on_connection(conn, req).await
    }

    /// Send a Request using the configured peer selection policy.
    pub async fn send(&self, req: DiameterMessage) -> Result<PeerResponseFuture> {
        let conn = self
            .select_peer()?
            .ok_or_else(|| Error::ClientError("No connected peers available".into()))?;
        self.send_on_connection(conn, req).await
    }

    fn select_peer(&self) -> Result<Option<Arc<PeerConnection>>> {
        let peers = self
            .inner
            .peers
            .lock()
            .map_err(|e| Error::LockError(e.to_string()))?;
        let connected: Vec<Arc<PeerConnection>> = peers
            .iter()
            .filter_map(|p| p.current_connection().filter(|c| c.is_connected()))
            .collect();
        if connected.is_empty() {
            return Ok(None);
        }
        match self.inner.config.selection {
            PeerSelection::Failover => Ok(Some(Arc::clone(&connected[0]))),
            PeerSelection::RoundRobin => {
                let idx = self.inner.rr_index.fetch_add(1, Ordering::Relaxed) as usize;
                Ok(Some(Arc::clone(&connected[idx % connected.len()])))
            }
        }
    }

    async fn send_on_connection(
        &self,
        conn: Arc<PeerConnection>,
        mut req: DiameterMessage,
    ) -> Result<PeerResponseFuture> {
        if req.get_flags() & flags::REQUEST == 0 {
            return Err(Error::ClientError(
                "send/send_to require a Request (R flag set)".into(),
            ));
        }

        let (tx, rx) = oneshot::channel();
        let hop_by_hop = {
            let mut pending = conn
                .pending
                .lock()
                .map_err(|e| Error::LockError(e.to_string()))?;
            if pending.len() >= self.inner.config.max_pending {
                return Err(Error::ClientError(format!(
                    "Peer {} pending queue full (max_pending={})",
                    conn.peer_name, self.inner.config.max_pending
                )));
            }
            // Allocate a free hop-by-hop id.
            let mut hbh = conn.next_hbh.fetch_add(1, Ordering::Relaxed) as u32;
            if hbh == 0 {
                hbh = conn.next_hbh.fetch_add(1, Ordering::Relaxed) as u32;
            }
            let start = hbh;
            while pending.contains_key(&hbh) {
                hbh = conn.next_hbh.fetch_add(1, Ordering::Relaxed) as u32;
                if hbh == 0 {
                    hbh = conn.next_hbh.fetch_add(1, Ordering::Relaxed) as u32;
                }
                if hbh == start {
                    return Err(Error::ClientError(
                        "No free hop-by-hop identifiers available".into(),
                    ));
                }
            }
            pending.insert(hbh, PendingEntry { sender: tx });
            hbh
        };

        req.set_hop_by_hop_id(hop_by_hop);

        if let Err(e) = conn.send_raw(&req).await {
            if let Ok(mut pending) = conn.pending.lock() {
                pending.remove(&hop_by_hop);
            }
            return Err(e);
        }

        Ok(PeerResponseFuture {
            receiver: Some(rx),
            connection: conn,
            hop_by_hop,
            deadline: Box::pin(sleep(self.inner.config.request_timeout)),
            finished: false,
        })
    }
}

fn make_base_answer(
    req: &DiameterMessage,
    result_code: u32,
    dict: Arc<Dictionary>,
) -> DiameterMessage {
    let mut res = DiameterMessage::new(
        req.get_command_code(),
        req.get_application_id(),
        req.get_flags() & !flags::REQUEST,
        req.get_hop_by_hop_id(),
        req.get_end_to_end_id(),
        dict,
    );
    res.add_avp(268, None, M, Unsigned32::new(result_code).into());
    res
}

/// Helper for tests / apps: wait until an inbound request is available or timeout.
pub async fn recv_incoming(
    rx: &mut mpsc::Receiver<IncomingRequest>,
    wait: Duration,
) -> Result<IncomingRequest> {
    match timeout(wait, rx.recv()).await {
        Ok(Some(req)) => Ok(req),
        Ok(None) => Err(Error::ClientError("Inbound channel closed".into())),
        Err(_) => Err(Error::ClientError("Timed out waiting for inbound request".into())),
    }
}
