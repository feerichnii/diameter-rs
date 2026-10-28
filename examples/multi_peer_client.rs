//! Active/Active Diameter client example (topology A).
//!
//! One `DiameterPeerClient` opens outbound connections to two peers, optionally
//! sharing a single local IP:port. Both peers can answer our Requests **and**
//! send their own Requests on those same TCP connections.
//!
//! Limitations (not production-ready Diameter peer state machine):
//! - Incoming DWR is answered with DWA automatically on the same connection
//!   (link-local; configure `origin_host`/`origin_realm`). CER/CEA and DPR/DPA
//!   are not automatic — handle via `take_incoming_requests` if peers send them.
//! - `send()` RoundRobin does not pin Session-Id to a peer; use `send_to` for
//!   stateful CCR-I/U/T sequences.
//! - Reconnecting to the same remote from a shared local port may hit TIME_WAIT.

use diameter::avp::flags::M;
use diameter::avp::{Identity, Unsigned32, UTF8String};
use diameter::dictionary::{self, Dictionary};
use diameter::flags;
use diameter::transport::{
    DiameterPeerClient, IncomingRequest, PeerClientConfig, PeerConfig, PeerSelection,
};
use diameter::{ApplicationId, CommandCode, DiameterMessage};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let dict = Arc::new(Dictionary::new(&[&dictionary::DEFAULT_DICT_XML]));

    // Shared source endpoint (Linux). Omit local_addr() for ephemeral ports.
    let local: SocketAddr = "127.0.0.1:14000".parse().unwrap();
    let config = PeerClientConfig::new()
        .local_addr(local)
        .origin_host("client.example.com")
        .origin_realm("example.com")
        .selection(PeerSelection::Failover)
        .request_timeout(Duration::from_secs(5));

    let client = DiameterPeerClient::new(Arc::clone(&dict), config).unwrap();
    let mut inbound = client.take_incoming_requests().expect("inbound receiver");

    client
        .add_peer(PeerConfig::new("a", "127.0.0.1:3868"))
        .unwrap();
    client
        .add_peer(PeerConfig::new("b", "127.0.0.1:3869"))
        .unwrap();

    for (name, result) in client.connect_all().await {
        match result {
            Ok(()) => log::info!("connected to peer {name}"),
            Err(e) => log::error!("failed to connect to peer {name}: {e}"),
        }
    }

    // Handle peer-initiated Requests (RAR/ASR/…) concurrently.
    // DWR never shows up here — the reader answers DWA itself.
    let dict_in = Arc::clone(&dict);
    tokio::spawn(async move {
        while let Some(req) = inbound.recv().await {
            handle_incoming(req, Arc::clone(&dict_in)).await;
        }
    });

    // Outgoing Request to a specific peer.
    if client.is_connected("a") {
        let fut = client
            .send_to("a", ccr(Arc::clone(&dict), 1))
            .await
            .unwrap();
        match fut.await {
            Ok(cca) => log::info!("answer from a: {}", cca),
            Err(e) => log::error!("request to a failed: {e}"),
        }
    }

    // Policy send (Failover / RoundRobin) — picks one connected peer, not broadcast.
    if let Ok(fut) = client.send(ccr(Arc::clone(&dict), 2)).await {
        match fut.await {
            Ok(cca) => log::info!("policy send answer: {}", cca),
            Err(e) => log::error!("policy send failed: {e}"),
        }
    }
}

async fn handle_incoming(req: IncomingRequest, dict: Arc<Dictionary>) {
    log::info!(
        "incoming {:?} from peer {} hbh={}",
        req.message.get_command_code(),
        req.peer_name,
        req.hop_by_hop_id()
    );
    let mut ans = DiameterMessage::new(
        req.message.get_command_code(),
        req.message.get_application_id(),
        req.message.get_flags() ^ flags::REQUEST,
        req.message.get_hop_by_hop_id(),
        req.message.get_end_to_end_id(),
        dict,
    );
    ans.add_avp(264, None, M, Identity::new("client.example.com").into());
    ans.add_avp(296, None, M, Identity::new("example.com").into());
    ans.add_avp(268, None, M, Unsigned32::new(2001).into());
    if let Err(e) = req.respond(ans).await {
        log::error!("respond failed: {e}");
    }
}

fn ccr(dict: Arc<Dictionary>, e2e: u32) -> DiameterMessage {
    let mut msg = DiameterMessage::new(
        CommandCode::CreditControl,
        ApplicationId::CreditControl,
        flags::REQUEST,
        0,
        e2e,
        dict,
    );
    msg.add_avp(264, None, M, Identity::new("client.example.com").into());
    msg.add_avp(296, None, M, Identity::new("example.com").into());
    msg.add_avp(263, None, M, UTF8String::new("ses;multi").into());
    msg.add_avp(416, None, M, Unsigned32::new(1).into());
    msg
}
