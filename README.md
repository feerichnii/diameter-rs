# Diameter

Rust Implementation of the Diameter Protocol.

[![Crates.io][crates-badge]][crates-url]
[![MIT licensed][mit-badge]][mit-url]
[![Build Status][actions-badge]][actions-url]

[crates-badge]: https://img.shields.io/crates/v/diameter.svg
[crates-url]: https://crates.io/crates/diameter
[mit-badge]: https://img.shields.io/badge/license-MIT-blue.svg
[mit-url]: LICENSE
[actions-badge]: https://github.com/lwlee2608/diameter-rs/actions/workflows/build.yml/badge.svg
[actions-url]: https://github.com/lwlee2608/diameter-rs/actions?query=branch%3Amaster

## Overview

This library provides a Rust implementation of the Diameter protocol, as defined by [RFC 6733](https://tools.ietf.org/html/rfc6733).



## Getting Started

### Installation
Add this crate to your Rust project by adding the following to your `Cargo.toml`:

```toml
[dependencies]
diameter-rs = "^0.7"
```


## Usage

### Diameter Client Example
Below is an example of creating a Diameter client that sends a Credit-Control-Request (CCR) message to a server and waits for a response.


```rust
use diameter::avp::flags::M;
use diameter::avp::Enumerated;
use diameter::avp::Identity;
use diameter::avp::UTF8String;
use diameter::avp::Unsigned32;
use diameter::dictionary::{self, Dictionary};
use diameter::flags;
use diameter::transport::DiameterClient;
use diameter::transport::DiameterClientConfig;
use diameter::{ApplicationId, CommandCode, DiameterMessage};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    // Diameter Dictionary
    let dict = Dictionary::new(&[&dictionary::DEFAULT_DICT_XML]);
    let dict = Arc::new(dict);

    // Initialize a Diameter client and connect it to the server
    let client_config = DiameterClientConfig {
        use_tls: false,
        verify_cert: false,
    };
    let mut client = DiameterClient::new("localhost:3868", client_config);
    let mut handler = client.connect().await.unwrap();
    let dict_ref = Arc::clone(&dict);
    tokio::spawn(async move {
        DiameterClient::handle(&mut handler, dict_ref).await;
    });

    // Create a Credit-Control-Request (CCR) Diameter message
    let mut ccr = DiameterMessage::new(
        CommandCode::CreditControl,
        ApplicationId::CreditControl,
        flags::REQUEST,
        1123158611,
        3102381851,
        dict,
    );
    ccr.add_avp(264, None, M, Identity::new("host.example.com").into());
    ccr.add_avp(296, None, M, Identity::new("realm.example.com").into());
    ccr.add_avp(263, None, M, UTF8String::new("ses;12345888").into());
    ccr.add_avp(416, None, M, Enumerated::new(1).into());
    ccr.add_avp(415, None, M, Unsigned32::new(1000).into());

    // Send the CCR message to the server and wait for a response
    let response = client.send_message(ccr).await.unwrap();
    let cca = response.await.unwrap();
    println!("Received response: {}", cca);
}

```


### Diameter Server Example
Below is an example of setting up a Diameter server that listens for incoming requests

```rust
use diameter::avp::flags::M;
use diameter::avp::Enumerated;
use diameter::avp::Identity;
use diameter::avp::UTF8String;
use diameter::avp::Unsigned32;
use diameter::dictionary::{self, Dictionary};
use diameter::flags;
use diameter::transport::DiameterServer;
use diameter::transport::DiameterServerConfig;
use diameter::DiameterMessage;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    // Diameter Dictionary
    let dict = Dictionary::new(&[&dictionary::DEFAULT_DICT_XML]);
    let dict = Arc::new(dict);

    // Set up a Diameter server listening on a specific port
    let mut server = DiameterServer::new("0.0.0.0:3868", DiameterServerConfig { native_tls: None })
        .await
        .unwrap();

    // Asynchronously handle incoming requests to the server
    let dict_ref = Arc::clone(&dict);
    server
        .listen(
            move |req| {
                let dict_ref2 = Arc::clone(&dict);
                async move {
                    println!("Received request: {}", req);

                    // Create a response message based on the received request
                    let mut res = DiameterMessage::new(
                        req.get_command_code(),
                        req.get_application_id(),
                        req.get_flags() ^ flags::REQUEST,
                        req.get_hop_by_hop_id(),
                        req.get_end_to_end_id(),
                        dict_ref2,
                    );

                    // Add various Attribute-Value Pairs (AVPs) to the response
                    res.add_avp(264, None, M, Identity::new("host.example.com").into());
                    res.add_avp(296, None, M, Identity::new("realm.example.com").into());
                    res.add_avp(263, None, M, UTF8String::new("ses;123458890").into());
                    res.add_avp(416, None, M, Enumerated::new(1).into());
                    res.add_avp(415, None, M, Unsigned32::new(1000).into());
                    res.add_avp(268, None, M, Unsigned32::new(2001).into());

                    // Return the response
                    Ok(res)
                }
            },
            dict_ref,
        )
        .await
        .unwrap();
}
```


### Multi-peer Client (Active/Active)

`DiameterPeerClient` holds several peer connections in one object. Outgoing Requests go via `send_to(peer, …)` or policy `send(…)` (`Failover` / `RoundRobin`). Peer-initiated Requests on those same TCP connections are delivered through `take_incoming_requests()`, and `IncomingRequest::respond()` answers on the **originating** connection.

```rust
use diameter::transport::{
    DiameterPeerClient, PeerClientConfig, PeerConfig, PeerSelection,
};
use std::net::SocketAddr;
use std::sync::Arc;

let local: SocketAddr = "10.0.0.5:3868".parse().unwrap();
let client = DiameterPeerClient::new(
    Arc::clone(&dict),
    PeerClientConfig::new()
        .local_addr(local) // shared source IP:port (Linux); omit for ephemeral ports
        .selection(PeerSelection::Failover),
).unwrap();

let mut inbound = client.take_incoming_requests().unwrap();
client.add_peer(PeerConfig::new("a", "10.0.0.10:3868")).unwrap();
client.add_peer(PeerConfig::new("b", "10.0.0.20:3868")).unwrap();
client.connect_all().await;

// Both peers answering our requests:
let fut = client.send_to("a", ccr).await.unwrap();
let cca = fut.await.unwrap();

// Both peers sending their own Requests (RAR/ASR/…; DWR is answered
// automatically by the reader and never reaches this queue):
tokio::spawn(async move {
    while let Some(req) = inbound.recv().await {
        // build answer, then:
        // req.respond(answer).await.unwrap();
        let _ = req;
    }
});
```

See `examples/multi_peer_client.rs`.

**Notes**

- Legacy `DiameterClient` / `DiameterServer` configs are unchanged (one connection / classic listener).
- Shared `local_addr` requires a concrete IP and nonzero port; reuse options default on when it is set (Linux `SO_REUSEADDR` / `SO_REUSEPORT`).
- `send()` picks **one** connected peer — it is not a broadcast. Session affinity (CCR-I/U/T) is the application's responsibility when using RoundRobin.
- Reconnecting to the same remote from the same local port may fail during TCP `TIME_WAIT`.
- Watchdog is link-local: an incoming DWR is answered with DWA automatically, strictly on the connection it arrived on (set `origin_host` / `origin_realm` in `PeerClientConfig` so the DWA carries proper identity). It is never delivered to the inbound Request queue.
- This layer is still not a full RFC 6733 peer state machine: CER/CEA and DPR/DPA are not automatic (incoming DPR can be answered via the inbound Request API), and the client does not yet send its own DWRs. Do not treat Active/Active as production-ready without those exchanges.

## TLS

Below are examples of how to set up TLS for both the server and the client.

### Server Configuration with TLS
```rust
    let mut cert_file = File::open("server.crt").unwrap();
    let mut certs = vec![];
    cert_file.read_to_end(&mut certs).unwrap();

    let mut key_file = File::open("server.key").unwrap();
    let mut key = vec![];
    key_file.read_to_end(&mut key).unwrap();

    let pkcs8 = native_tls::Identity::from_pkcs8(&certs, &key).unwrap();
    let config = DiameterServerConfig {
        native_tls: Some(pkcs8),
    };
```

### Client Configuration with TLS
```rust
    let client_config = DiameterClientConfig {
        use_tls: true,
        verify_cert: false,
    };
```
