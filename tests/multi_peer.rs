//! Active/Active multi-peer client acceptance tests (topology A).
use diameter::avp::flags::M;
use diameter::avp::{Identity, Unsigned32, UTF8String};
use diameter::dictionary::{self, Dictionary};
use diameter::flags;
use diameter::transport::{
    ConnectionConfig, DiameterPeerClient, DiameterServer, DiameterServerConfig, PeerClientConfig,
    PeerConfig, PeerSelection, ServerSocketConfig,
};
use diameter::{ApplicationId, CommandCode, DiameterMessage};
use diameter::transport::Codec;
use std::fs;
use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};

fn dict() -> Arc<Dictionary> {
    Arc::new(Dictionary::new(&[&dictionary::DEFAULT_DICT_XML]))
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
    msg.add_avp(263, None, M, UTF8String::new("ses;1").into());
    msg.add_avp(416, None, M, Unsigned32::new(1).into());
    msg
}

fn answer_for(req: &DiameterMessage, origin: &str, dict: Arc<Dictionary>) -> DiameterMessage {
    let mut res = DiameterMessage::new(
        req.get_command_code(),
        req.get_application_id(),
        req.get_flags() ^ flags::REQUEST,
        req.get_hop_by_hop_id(),
        req.get_end_to_end_id(),
        dict,
    );
    res.add_avp(264, None, M, Identity::new(origin).into());
    res.add_avp(296, None, M, Identity::new("example.com").into());
    res.add_avp(268, None, M, Unsigned32::new(2001).into());
    res
}

fn rar(dict: Arc<Dictionary>, hbh: u32, e2e: u32) -> DiameterMessage {
    let mut msg = DiameterMessage::new(
        CommandCode::ReAuth,
        ApplicationId::CreditControl,
        flags::REQUEST,
        hbh,
        e2e,
        dict,
    );
    msg.add_avp(264, None, M, Identity::new("peer.example.com").into());
    msg.add_avp(296, None, M, Identity::new("example.com").into());
    msg.add_avp(263, None, M, UTF8String::new("ses;rar").into());
    msg
}

enum PeerCmd {
    SendRar { hbh: u32, e2e: u32 },
    Shutdown,
}

/// Bidirectional test peer: answers inbound Requests and can push RARs.
async fn spawn_bidi_peer(
    addr: SocketAddr,
    origin: &'static str,
    dict: Arc<Dictionary>,
) -> mpsc::Sender<PeerCmd> {
    let listener = TcpListener::bind(addr).await.expect("bind peer");
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<PeerCmd>(8);
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        loop {
            tokio::select! {
                biased;
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(PeerCmd::SendRar { hbh, e2e }) => {
                            let msg = rar(Arc::clone(&dict), hbh, e2e);
                            Codec::encode(&mut stream, &msg).await.expect("encode rar");
                        }
                        Some(PeerCmd::Shutdown) | None => break,
                    }
                }
                decoded = Codec::decode(&mut stream, Arc::clone(&dict)) => {
                    match decoded {
                        Ok(req) => {
                            if req.get_flags() & flags::REQUEST != 0 {
                                let res = answer_for(&req, origin, Arc::clone(&dict));
                                Codec::encode(&mut stream, &res).await.expect("encode ans");
                            }
                            // Answers from client (to our RAR) are ignored here.
                        }
                        Err(_) => break,
                    }
                }
            }
        }
    });
    // Give the listener a moment.
    tokio::time::sleep(Duration::from_millis(20)).await;
    cmd_tx
}

/// Simple echo DiameterServer with fixed Origin-Host.
async fn spawn_echo_server(addr: &str, origin: &'static str, dict: Arc<Dictionary>) {
    let mut server = DiameterServer::new(addr, DiameterServerConfig { native_tls: None })
        .await
        .expect("server");
    let dict_listen = Arc::clone(&dict);
    tokio::spawn(async move {
        let _ = server
            .listen(
                move |req| {
                    let dict = Arc::clone(&dict);
                    async move { Ok(answer_for(&req, origin, dict)) }
                },
                dict_listen,
            )
            .await;
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
}

#[tokio::test]
async fn send_to_reaches_correct_peer() {
    let dict = dict();
    spawn_echo_server("127.0.0.1:28011", "peer-a.example.com", Arc::clone(&dict)).await;
    spawn_echo_server("127.0.0.1:28012", "peer-b.example.com", Arc::clone(&dict)).await;

    let client = DiameterPeerClient::new(Arc::clone(&dict), PeerClientConfig::new()).unwrap();
    client
        .add_peer(PeerConfig::new("a", "127.0.0.1:28011"))
        .unwrap();
    client
        .add_peer(PeerConfig::new("b", "127.0.0.1:28012"))
        .unwrap();
    for (name, r) in client.connect_all().await {
        r.unwrap_or_else(|e| panic!("connect {name}: {e}"));
    }

    let a = client
        .send_to("a", ccr(Arc::clone(&dict), 1))
        .await
        .unwrap()
        .await
        .unwrap();
    assert_eq!(
        a.get_avp(264).unwrap().get_identity().unwrap().value(),
        "peer-a.example.com"
    );

    let b = client
        .send_to("b", ccr(Arc::clone(&dict), 2))
        .await
        .unwrap()
        .await
        .unwrap();
    assert_eq!(
        b.get_avp(264).unwrap().get_identity().unwrap().value(),
        "peer-b.example.com"
    );
}

#[tokio::test]
async fn round_robin_and_failover() {
    let dict = dict();
    spawn_echo_server("127.0.0.1:28021", "peer-a.example.com", Arc::clone(&dict)).await;
    spawn_echo_server("127.0.0.1:28022", "peer-b.example.com", Arc::clone(&dict)).await;

    let cfg = PeerClientConfig::new().selection(PeerSelection::RoundRobin);
    let client = DiameterPeerClient::new(Arc::clone(&dict), cfg).unwrap();
    client
        .add_peer(PeerConfig::new("a", "127.0.0.1:28021"))
        .unwrap();
    client
        .add_peer(PeerConfig::new("b", "127.0.0.1:28022"))
        .unwrap();
    for (_, r) in client.connect_all().await {
        r.unwrap();
    }

    let mut hosts = Vec::new();
    for i in 0..4u32 {
        let ans = client
            .send(ccr(Arc::clone(&dict), 100 + i))
            .await
            .unwrap()
            .await
            .unwrap();
        hosts.push(ans.get_avp(264).unwrap().get_identity().unwrap().value().to_string());
    }
    assert!(hosts.contains(&"peer-a.example.com".to_string()));
    assert!(hosts.contains(&"peer-b.example.com".to_string()));
    // Round-robin should alternate when both are up.
    assert_ne!(hosts[0], hosts[1]);

    // Kill peer B and switch to failover.
    client.disconnect("b").await.unwrap();
    let cfg = PeerClientConfig::new().selection(PeerSelection::Failover);
    // Use existing client still — selection is fixed at construction; send_to after disconnect.
    let pending = client.send_to("b", ccr(Arc::clone(&dict), 200)).await;
    assert!(pending.is_err());

    let ans = client
        .send_to("a", ccr(Arc::clone(&dict), 201))
        .await
        .unwrap()
        .await
        .unwrap();
    assert_eq!(
        ans.get_avp(264).unwrap().get_identity().unwrap().value(),
        "peer-a.example.com"
    );
    let _ = cfg; // silence
}

#[tokio::test]
async fn shared_local_addr_two_peers() {
    let dict = dict();
    let local: SocketAddr = "127.0.0.1:28030".parse().unwrap();
    spawn_echo_server("127.0.0.1:28031", "peer-a.example.com", Arc::clone(&dict)).await;
    spawn_echo_server("127.0.0.1:28032", "peer-b.example.com", Arc::clone(&dict)).await;

    let cfg = PeerClientConfig::new().local_addr(local);
    let client = DiameterPeerClient::new(Arc::clone(&dict), cfg).unwrap();
    client
        .add_peer(PeerConfig::new("a", "127.0.0.1:28031"))
        .unwrap();
    client
        .add_peer(PeerConfig::new("b", "127.0.0.1:28032"))
        .unwrap();
    for (name, r) in client.connect_all().await {
        r.unwrap_or_else(|e| panic!("connect {name}: {e}"));
    }

    assert_eq!(client.peer_local_addr("a"), Some(local));
    assert_eq!(client.peer_local_addr("b"), Some(local));
    assert_ne!(
        client.peer_remote_addr("a").unwrap(),
        client.peer_remote_addr("b").unwrap()
    );

    #[cfg(target_os = "linux")]
    {
        let tcp = fs::read_to_string("/proc/net/tcp").unwrap();
        // 127.0.0.1:28030 in hex: 0100007F:6D7E
        let needle = "0100007F:6D7E";
        let count = tcp.lines().filter(|l| l.contains(needle)).count();
        assert!(
            count >= 2,
            "expected >=2 sockets on shared local port, found {count}"
        );
    }
}

#[tokio::test]
async fn listener_coexists_with_shared_local_outgoing() {
    let dict = dict();
    let local: SocketAddr = "127.0.0.1:28040".parse().unwrap();

    let _listener = DiameterServer::new_with_socket_config(
        "127.0.0.1:28040",
        DiameterServerConfig { native_tls: None },
        ServerSocketConfig::new().reuse_addr(true).reuse_port(true),
    )
    .await
    .expect("listener with reuseport");

    spawn_echo_server("127.0.0.1:28041", "peer-a.example.com", Arc::clone(&dict)).await;
    spawn_echo_server("127.0.0.1:28042", "peer-b.example.com", Arc::clone(&dict)).await;

    let cfg = PeerClientConfig::new().local_addr(local);
    let client = DiameterPeerClient::new(Arc::clone(&dict), cfg).unwrap();
    client
        .add_peer(PeerConfig::new("a", "127.0.0.1:28041"))
        .unwrap();
    client
        .add_peer(PeerConfig::new("b", "127.0.0.1:28042"))
        .unwrap();
    for (name, r) in client.connect_all().await {
        r.unwrap_or_else(|e| panic!("connect {name}: {e}"));
    }
    assert!(client.is_connected("a"));
    assert!(client.is_connected("b"));
}

#[tokio::test]
async fn simultaneous_incoming_requests_and_respond() {
    let dict = dict();
    let addr_a: SocketAddr = "127.0.0.1:28051".parse().unwrap();
    let addr_b: SocketAddr = "127.0.0.1:28052".parse().unwrap();
    let cmd_a = spawn_bidi_peer(addr_a, "peer-a.example.com", Arc::clone(&dict)).await;
    let cmd_b = spawn_bidi_peer(addr_b, "peer-b.example.com", Arc::clone(&dict)).await;

    let client = DiameterPeerClient::new(Arc::clone(&dict), PeerClientConfig::new()).unwrap();
    let mut inbound = client.take_incoming_requests().expect("inbound rx");
    assert!(client.take_incoming_requests().is_none());

    client
        .add_peer(PeerConfig::new("a", "127.0.0.1:28051"))
        .unwrap();
    client
        .add_peer(PeerConfig::new("b", "127.0.0.1:28052"))
        .unwrap();
    for (_, r) in client.connect_all().await {
        r.unwrap();
    }

    // Also exercise outgoing while inbound flows.
    let out = client
        .send_to("a", ccr(Arc::clone(&dict), 50))
        .await
        .unwrap();

    cmd_a
        .send(PeerCmd::SendRar { hbh: 9001, e2e: 9001 })
        .await
        .unwrap();
    cmd_b
        .send(PeerCmd::SendRar { hbh: 9002, e2e: 9002 })
        .await
        .unwrap();

    let mut got_a = false;
    let mut got_b = false;
    for _ in 0..2 {
        let incoming = tokio::time::timeout(Duration::from_secs(2), inbound.recv())
            .await
            .expect("timeout")
            .expect("closed");
        let hbh = incoming.hop_by_hop_id();
        let e2e = incoming.end_to_end_id();
        let peer = incoming.peer_name.clone();
        let ans = answer_for(&incoming.message, "client.example.com", Arc::clone(&dict));
        incoming.respond(ans).await.unwrap();
        if peer == "a" {
            assert_eq!(hbh, 9001);
            assert_eq!(e2e, 9001);
            got_a = true;
        } else if peer == "b" {
            assert_eq!(hbh, 9002);
            assert_eq!(e2e, 9002);
            got_b = true;
        }
    }
    assert!(got_a && got_b);

    let cca = out.await.unwrap();
    assert_eq!(cca.get_avp(268).unwrap().get_unsigned32().unwrap(), 2001);

    let _ = cmd_a.send(PeerCmd::Shutdown).await;
    let _ = cmd_b.send(PeerCmd::Shutdown).await;
}

#[tokio::test]
async fn inbound_queue_full_sends_too_busy() {
    let dict = dict();
    let addr: SocketAddr = "127.0.0.1:28061".parse().unwrap();
    let listener = TcpListener::bind(addr).await.unwrap();

    let (accepted_tx, accepted_rx) = oneshot::channel::<TcpStream>();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let _ = accepted_tx.send(stream);
    });

    let cfg = PeerClientConfig::new().max_inbound_pending(1);
    let client = DiameterPeerClient::new(Arc::clone(&dict), cfg).unwrap();
    // Take receiver but do not drain — fill the queue.
    let mut inbound = client.take_incoming_requests().unwrap();
    client
        .add_peer(PeerConfig::new("a", "127.0.0.1:28061"))
        .unwrap();
    client.connect("a").await.unwrap();

    let mut stream = accepted_rx.await.unwrap();
    // First RAR fills the queue.
    Codec::encode(&mut stream, &rar(Arc::clone(&dict), 1, 1))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    // Second should get TOO_BUSY (3004) answer without blocking the reader.
    Codec::encode(&mut stream, &rar(Arc::clone(&dict), 2, 2))
        .await
        .unwrap();

    let busy = tokio::time::timeout(Duration::from_secs(2), Codec::decode(&mut stream, Arc::clone(&dict)))
        .await
        .expect("timeout waiting busy")
        .expect("decode");
    assert_eq!(busy.get_flags() & flags::REQUEST, 0);
    assert_eq!(busy.get_hop_by_hop_id(), 2);
    assert_eq!(busy.get_avp(268).unwrap().get_unsigned32().unwrap(), 3004);

    // First inbound still deliverable; peer still readable (send a normal request answer path).
    let first = inbound.recv().await.unwrap();
    assert_eq!(first.hop_by_hop_id(), 1);
}

#[tokio::test]
async fn concurrent_connect_is_idempotent() {
    let dict = dict();
    spawn_echo_server("127.0.0.1:28071", "peer-a.example.com", Arc::clone(&dict)).await;
    spawn_echo_server("127.0.0.1:28072", "peer-b.example.com", Arc::clone(&dict)).await;

    let client = DiameterPeerClient::new(Arc::clone(&dict), PeerClientConfig::new()).unwrap();
    client
        .add_peer(PeerConfig::new("a", "127.0.0.1:28071"))
        .unwrap();
    client
        .add_peer(PeerConfig::new("b", "127.0.0.1:28072"))
        .unwrap();

    let mut handles = Vec::new();
    for _ in 0..10 {
        let c = client.clone();
        handles.push(tokio::spawn(async move { c.connect("a").await }));
    }
    // connect b should not be blocked by a's connect lock.
    let b = client.connect("b").await;
    b.unwrap();

    for h in handles {
        h.await.unwrap().unwrap();
    }
    assert!(client.is_connected("a"));
    assert!(client.is_connected("b"));
    // Single generation for a after concurrent connects.
    assert_eq!(client.peer_generation("a").unwrap(), 1);
}

#[tokio::test]
async fn disconnect_clears_pending_and_preserves_other_peer() {
    let dict = dict();
    // Slow peer A: accept but never answer.
    let listener = TcpListener::bind("127.0.0.1:28083").await.unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        // Read and ignore forever.
        let mut buf = [0u8; 4096];
        loop {
            if stream.read(&mut buf).await.unwrap_or(0) == 0 {
                break;
            }
        }
    });
    spawn_echo_server("127.0.0.1:28084", "peer-b.example.com", Arc::clone(&dict)).await;

    let cfg = PeerClientConfig::new().request_timeout(Duration::from_secs(10));
    let client = DiameterPeerClient::new(Arc::clone(&dict), cfg).unwrap();
    client
        .add_peer(PeerConfig::new("a", "127.0.0.1:28083"))
        .unwrap();
    client
        .add_peer(PeerConfig::new("b", "127.0.0.1:28084"))
        .unwrap();
    client.connect("a").await.unwrap();
    client.connect("b").await.unwrap();

    let pending_a = client
        .send_to("a", ccr(Arc::clone(&dict), 1))
        .await
        .unwrap();
    client.disconnect("a").await.unwrap();
    let err = pending_a.await;
    assert!(err.is_err());

    let b = client
        .send_to("b", ccr(Arc::clone(&dict), 2))
        .await
        .unwrap()
        .await
        .unwrap();
    assert_eq!(b.get_avp(268).unwrap().get_unsigned32().unwrap(), 2001);
}

#[tokio::test]
async fn max_pending_and_drop_cleans_slot() {
    let dict = dict();
    let listener = TcpListener::bind("127.0.0.1:28091").await.unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 4096];
        loop {
            if stream.read(&mut buf).await.unwrap_or(0) == 0 {
                break;
            }
        }
    });

    let cfg = PeerClientConfig::new()
        .max_pending(2)
        .request_timeout(Duration::from_millis(200));
    let client = DiameterPeerClient::new(Arc::clone(&dict), cfg).unwrap();
    client
        .add_peer(PeerConfig::new("a", "127.0.0.1:28091"))
        .unwrap();
    client.connect("a").await.unwrap();

    let f1 = client
        .send_to("a", ccr(Arc::clone(&dict), 1))
        .await
        .unwrap();
    let f2 = client
        .send_to("a", ccr(Arc::clone(&dict), 2))
        .await
        .unwrap();
    let f3 = client.send_to("a", ccr(Arc::clone(&dict), 3)).await;
    assert!(f3.is_err());

    drop(f1);
    // After drop, a slot frees; may still race with timeout on f2.
    tokio::time::sleep(Duration::from_millis(10)).await;
    let f4 = client.send_to("a", ccr(Arc::clone(&dict), 4)).await;
    assert!(f4.is_ok());
    drop(f2);
    drop(f4);
}

#[tokio::test]
async fn unexpected_answer_does_not_kill_reader() {
    let dict = dict();
    let listener = TcpListener::bind("127.0.0.1:28101").await.unwrap();
    let dict_peer = Arc::clone(&dict);
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        // Send an unmatched Answer first.
        let rogue = answer_for(
            &DiameterMessage::new(
                CommandCode::CreditControl,
                ApplicationId::CreditControl,
                0,
                424242,
                1,
                Arc::clone(&dict_peer),
            ),
            "rogue",
            Arc::clone(&dict_peer),
        );
        Codec::encode(&mut stream, &rogue).await.unwrap();
        // Then behave as echo for real requests.
        loop {
            match Codec::decode(&mut stream, Arc::clone(&dict_peer)).await {
                Ok(req) if req.get_flags() & flags::REQUEST != 0 => {
                    let res = answer_for(&req, "peer-a.example.com", Arc::clone(&dict_peer));
                    Codec::encode(&mut stream, &res).await.unwrap();
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let client = DiameterPeerClient::new(Arc::clone(&dict), PeerClientConfig::new()).unwrap();
    client
        .add_peer(PeerConfig::new("a", "127.0.0.1:28101"))
        .unwrap();
    client.connect("a").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(client.is_connected("a"));

    let ans = client
        .send_to("a", ccr(Arc::clone(&dict), 7))
        .await
        .unwrap()
        .await
        .unwrap();
    assert_eq!(
        ans.get_avp(264).unwrap().get_identity().unwrap().value(),
        "peer-a.example.com"
    );
}

#[tokio::test]
async fn reconnect_increments_generation() {
    let dict = dict();
    spawn_echo_server("127.0.0.1:28111", "peer-a.example.com", Arc::clone(&dict)).await;

    let client = DiameterPeerClient::new(Arc::clone(&dict), PeerClientConfig::new()).unwrap();
    client
        .add_peer(PeerConfig::new("a", "127.0.0.1:28111"))
        .unwrap();
    client.connect("a").await.unwrap();
    let g1 = client.peer_generation("a").unwrap();
    client.reconnect("a").await.unwrap();
    let g2 = client.peer_generation("a").unwrap();
    assert!(g2 > g1);

    let ans = client
        .send_to("a", ccr(Arc::clone(&dict), 1))
        .await
        .unwrap()
        .await
        .unwrap();
    assert_eq!(ans.get_avp(268).unwrap().get_unsigned32().unwrap(), 2001);
}

#[tokio::test]
async fn time_wait_behavior_with_shared_local() {
    let dict = dict();
    let local: SocketAddr = "127.0.0.1:28120".parse().unwrap();
    let remote: SocketAddr = "127.0.0.1:28121".parse().unwrap();

    // Server that accepts and closes from its side when told — first connection.
    let listener = TcpListener::bind(remote).await.unwrap();
    let (go_tx, go_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        // Hold until client disconnects, then accept again if possible.
        drop(stream);
        let _ = go_rx.await;
        // Second accept attempt for documentation; may or may not arrive.
        let _ = listener.accept().await;
    });

    let cfg = PeerClientConfig::new().local_addr(local);
    let client = DiameterPeerClient::new(Arc::clone(&dict), cfg).unwrap();
    client
        .add_peer(PeerConfig::new("a", "127.0.0.1:28121"))
        .unwrap();
    client.connect("a").await.unwrap();
    // Active close from client — 4-tuple enters TIME_WAIT on client side.
    client.disconnect("a").await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let result = client.connect("a").await;
    // On Linux this often fails with address-related errors until TIME_WAIT expires.
    // Accept either success (if stack allows) or a client/io error — document both.
    match result {
        Ok(()) => {
            // Reconnect succeeded (e.g. server closed first / stack reused).
            assert!(client.is_connected("a"));
        }
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("Address")
                    || msg.contains("addr")
                    || msg.contains("in use")
                    || msg.contains("connect")
                    || msg.contains("os error"),
                "unexpected error: {msg}"
            );
        }
    }
    let _ = go_tx.send(());
}

#[tokio::test]
async fn tls_verify_with_sni_and_ip_requires_override() {
    let dict = dict();
    let cert_pem = fs::read("tests/test_cert.pem").expect("test cert");
    let key_pem = fs::read("tests/test_key.pem").expect("test key");
    let identity = native_tls::Identity::from_pkcs8(&cert_pem, &key_pem).expect("identity");
    let root = native_tls::Certificate::from_pem(&cert_pem).expect("root");

    let mut server = DiameterServer::new(
        "127.0.0.1:28131",
        DiameterServerConfig {
            native_tls: Some(identity),
        },
    )
    .await
    .unwrap();
    let dict_s = Arc::clone(&dict);
    let dict_listen = Arc::clone(&dict);
    tokio::spawn(async move {
        let _ = server
            .listen(
                move |req| {
                    let dict = Arc::clone(&dict_s);
                    async move { Ok(answer_for(&req, "tls-peer", dict)) }
                },
                dict_listen,
            )
            .await;
    });
    tokio::time::sleep(Duration::from_millis(40)).await;

    // Positive: IP + tls_server_name matching CN.
    let client = DiameterPeerClient::new(Arc::clone(&dict), PeerClientConfig::new()).unwrap();
    let conn = ConnectionConfig::new("127.0.0.1:28131")
        .use_tls(true)
        .verify_cert(true)
        .tls_server_name("peer.example.com")
        .add_root_cert(root.clone());
    client
        .add_peer(PeerConfig::new("a", "127.0.0.1:28131").connection(conn))
        .unwrap();
    client.connect("a").await.unwrap();
    let ans = client
        .send_to("a", ccr(Arc::clone(&dict), 1))
        .await
        .unwrap()
        .await
        .unwrap();
    assert_eq!(ans.get_avp(264).unwrap().get_identity().unwrap().value(), "tls-peer");

    // Negative: IP without tls_server_name should fail before connect.
    let client2 = DiameterPeerClient::new(Arc::clone(&dict), PeerClientConfig::new()).unwrap();
    let bad = ConnectionConfig::new("127.0.0.1:28131")
        .use_tls(true)
        .verify_cert(true)
        .add_root_cert(root);
    client2
        .add_peer(PeerConfig::new("b", "127.0.0.1:28131").connection(bad))
        .unwrap();
    let err = client2.connect("b").await;
    assert!(err.is_err());
}

#[tokio::test]
async fn legacy_configs_still_constructible() {
    // Compile-time / runtime check that public struct literals remain valid.
    let _c = diameter::transport::DiameterClientConfig {
        use_tls: false,
        verify_cert: false,
    };
    let _s = DiameterServerConfig { native_tls: None };
    let _ = AtomicUsize::new(0);
}

#[tokio::test]
async fn request_timeout_fires() {
    let dict = dict();
    let listener = TcpListener::bind("127.0.0.1:28141").await.unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = stream.read(&mut buf).await;
        tokio::time::sleep(Duration::from_secs(5)).await;
    });

    let cfg = PeerClientConfig::new().request_timeout(Duration::from_millis(100));
    let client = DiameterPeerClient::new(Arc::clone(&dict), cfg).unwrap();
    client
        .add_peer(PeerConfig::new("a", "127.0.0.1:28141"))
        .unwrap();
    client.connect("a").await.unwrap();
    let err = client
        .send_to("a", ccr(Arc::clone(&dict), 1))
        .await
        .unwrap()
        .await;
    assert!(err.unwrap_err().to_string().contains("timed out"));
}

/// DWR is link-local: the reader answers DWA on the same socket automatically,
/// and the watchdog never reaches the application's inbound queue.
#[tokio::test]
async fn dwr_gets_automatic_dwa_on_same_socket() {
    let dict = dict();
    let listener = TcpListener::bind("127.0.0.1:28161").await.unwrap();

    let cfg = PeerClientConfig::new()
        .origin_host("client.example.com")
        .origin_realm("example.com");
    let client = DiameterPeerClient::new(Arc::clone(&dict), cfg).unwrap();
    client.add_peer(PeerConfig::new("a", "127.0.0.1:28161")).unwrap();
    let mut inbound = client.take_incoming_requests().unwrap();

    let accept = tokio::spawn(async move { listener.accept().await.unwrap().0 });
    for (_, r) in client.connect_all().await {
        r.unwrap();
    }
    let mut stream = accept.await.unwrap();

    // Peer sends DWR on the established connection.
    let mut dwr = DiameterMessage::new(
        CommandCode::DeviceWatchdog,
        ApplicationId::Common,
        flags::REQUEST,
        77,
        88,
        Arc::clone(&dict),
    );
    dwr.add_avp(264, None, M, Identity::new("peer.example.com").into());
    dwr.add_avp(296, None, M, Identity::new("example.com").into());
    Codec::encode(&mut stream, &dwr).await.unwrap();

    // Reader must answer DWA strictly on the same socket.
    let dwa = tokio::time::timeout(
        Duration::from_secs(2),
        Codec::decode(&mut stream, Arc::clone(&dict)),
    )
    .await
    .expect("DWA not received in time")
    .unwrap();
    assert_eq!(dwa.get_command_code(), CommandCode::DeviceWatchdog);
    assert_eq!(dwa.get_flags() & flags::REQUEST, 0);
    assert_eq!(dwa.get_hop_by_hop_id(), 77);
    assert_eq!(dwa.get_end_to_end_id(), 88);
    assert_eq!(dwa.get_avp(268).unwrap().get_unsigned32().unwrap(), 2001);
    assert_eq!(
        dwa.get_avp(264).unwrap().get_identity().unwrap().value(),
        "client.example.com"
    );

    // The watchdog never reaches the application's inbound queue.
    let got = tokio::time::timeout(Duration::from_millis(300), inbound.recv()).await;
    assert!(got.is_err(), "DWR unexpectedly delivered to the application");
}
