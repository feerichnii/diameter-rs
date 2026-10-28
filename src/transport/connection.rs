//! Shared connection helpers for Diameter transport.

use crate::error::{Error, Result};
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpSocket, TcpStream};
use tokio::time::timeout;

/// Per-peer connection settings (no local bind address — that belongs to the client).
#[derive(Clone)]
pub struct ConnectionConfig {
    pub address: String,
    pub use_tls: bool,
    pub verify_cert: bool,
    pub tls_server_name: Option<String>,
    pub tls_root_certs: Vec<native_tls::Certificate>,
    pub connect_timeout: Duration,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            address: String::new(),
            use_tls: false,
            verify_cert: true,
            tls_server_name: None,
            tls_root_certs: Vec::new(),
            connect_timeout: Duration::from_secs(10),
        }
    }
}

impl ConnectionConfig {
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            ..Default::default()
        }
    }

    pub fn use_tls(mut self, use_tls: bool) -> Self {
        self.use_tls = use_tls;
        self
    }

    pub fn verify_cert(mut self, verify_cert: bool) -> Self {
        self.verify_cert = verify_cert;
        self
    }

    pub fn tls_server_name(mut self, name: impl Into<String>) -> Self {
        self.tls_server_name = Some(name.into());
        self
    }

    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    pub fn add_root_cert(mut self, cert: native_tls::Certificate) -> Self {
        self.tls_root_certs.push(cert);
        self
    }
}

/// Socket option flags applied before `bind()`.
#[derive(Debug, Clone, Copy, Default)]
pub struct SocketOpts {
    pub reuse_addr: bool,
    pub reuse_port: bool,
}

impl SocketOpts {
    pub fn new() -> Self {
        Self::default()
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

/// Result of establishing a (possibly TLS-wrapped) TCP connection.
pub struct ConnectedStream {
    pub reader: Box<dyn AsyncRead + Send + Unpin>,
    pub writer: Box<dyn AsyncWrite + Send + Unpin>,
    pub local_addr: SocketAddr,
    pub peer_addr: SocketAddr,
}

/// Resolve `host:port` (or bare socket-addr string) to a concrete remote address.
pub(crate) async fn resolve_address(address: &str) -> Result<SocketAddr> {
    let mut iter = tokio::net::lookup_host(address)
        .await
        .map_err(|e| Error::ClientError(format!("Failed to resolve {}: {}", address, e)))?;
    iter.next()
        .ok_or_else(|| Error::ClientError(format!("No addresses resolved for {}", address)))
}

/// Hostname used for TLS SNI — never includes `:port`.
pub(crate) fn tls_sni_name(conn: &ConnectionConfig) -> Result<String> {
    if let Some(ref name) = conn.tls_server_name {
        return Ok(name.clone());
    }
    let host = strip_port(&conn.address)?;
    if host.parse::<IpAddr>().is_ok() {
        return Err(Error::ClientError(
            "tls_server_name is required when connecting to an IP address with TLS".into(),
        ));
    }
    Ok(host)
}

fn strip_port(address: &str) -> Result<String> {
    if let Some(rest) = address.strip_prefix('[') {
        // IPv6 literal: [addr]:port
        if let Some(end) = rest.find(']') {
            return Ok(rest[..end].to_string());
        }
        return Err(Error::ClientError(format!(
            "Invalid IPv6 address literal: {}",
            address
        )));
    }
    if let Some((host, _port)) = address.rsplit_once(':') {
        if !host.is_empty() {
            return Ok(host.to_string());
        }
    }
    Ok(address.to_string())
}

fn new_socket_for(addr: SocketAddr) -> Result<TcpSocket> {
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    Ok(socket)
}

pub(crate) fn apply_socket_opts(socket: &TcpSocket, opts: SocketOpts) -> Result<()> {
    if opts.reuse_addr {
        socket.set_reuseaddr(true)?;
    }
    if opts.reuse_port {
        #[cfg(unix)]
        socket.set_reuseport(true)?;
        #[cfg(not(unix))]
        {
            let _ = opts;
            return Err(Error::ClientError(
                "SO_REUSEPORT is not supported on this platform".into(),
            ));
        }
    }
    Ok(())
}

/// Bind + connect (with optional TLS) using `ConnectionConfig`.
pub(crate) async fn connect_stream(
    conn: &ConnectionConfig,
    local: Option<SocketAddr>,
    opts: SocketOpts,
) -> Result<ConnectedStream> {
    let remote = resolve_address(&conn.address).await?;
    let socket = new_socket_for(remote)?;
    apply_socket_opts(&socket, opts)?;

    if let Some(local_addr) = local {
        if local_addr.is_ipv4() != remote.is_ipv4() {
            return Err(Error::ClientError(format!(
                "local_addr family {} does not match remote {}",
                local_addr, remote
            )));
        }
        socket.bind(local_addr)?;
    }

    let stream = timeout(conn.connect_timeout, socket.connect(remote))
        .await
        .map_err(|_| {
            Error::ClientError(format!(
                "Connect to {} timed out after {:?}",
                conn.address, conn.connect_timeout
            ))
        })??;

    let local_addr = stream.local_addr()?;
    let peer_addr = stream.peer_addr()?;

    if conn.use_tls {
        split_tls(conn, stream, local_addr, peer_addr).await
    } else {
        let (reader, writer) = tokio::io::split(stream);
        Ok(ConnectedStream {
            reader: Box::new(reader),
            writer: Box::new(writer),
            local_addr,
            peer_addr,
        })
    }
}

async fn split_tls(
    conn: &ConnectionConfig,
    stream: TcpStream,
    local_addr: SocketAddr,
    peer_addr: SocketAddr,
) -> Result<ConnectedStream> {
    let sni = tls_sni_name(conn)?;
    let mut builder = native_tls::TlsConnector::builder();
    builder.danger_accept_invalid_certs(!conn.verify_cert);
    for cert in &conn.tls_root_certs {
        builder.add_root_certificate(cert.clone());
    }
    let tls_connector = tokio_native_tls::TlsConnector::from(builder.build()?);
    let tls_stream = timeout(conn.connect_timeout, tls_connector.connect(&sni, stream))
        .await
        .map_err(|_| {
            Error::ClientError(format!(
                "TLS handshake to {} timed out after {:?}",
                conn.address, conn.connect_timeout
            ))
        })?
        .map_err(|e| Error::ClientError(format!("TLS handshake failed: {}", e)))?;

    let (reader, writer) = tokio::io::split(tls_stream);
    Ok(ConnectedStream {
        reader: Box::new(reader),
        writer: Box::new(writer),
        local_addr,
        peer_addr,
    })
}

/// Validate a shared local endpoint: concrete IP (not unspecified) and nonzero port.
pub(crate) fn validate_shared_local_addr(addr: SocketAddr) -> Result<()> {
    if addr.port() == 0 {
        return Err(Error::ClientError(
            "local_addr port must be nonzero for a shared source endpoint".into(),
        ));
    }
    if addr.ip().is_unspecified() {
        return Err(Error::ClientError(
            "local_addr must be a concrete IP (not 0.0.0.0 or ::) for a shared source endpoint"
                .into(),
        ));
    }
    Ok(())
}
