use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::Instant,
};

use anyhow::{Context, Result, ensure};
use serde::Serialize;
use tokio::net::{TcpSocket, TcpStream, lookup_host};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, client_async_tls_with_config,
    tungstenite::handshake::client::Request,
};

pub type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Clone, Debug, Serialize)]
pub struct ConnectionInfo {
    pub local_addr: SocketAddr,
    pub peer_addr: SocketAddr,
    pub handshake_us: u64,
    pub requested_port: Option<u16>,
}

/// Explicit local binding is for controlled experiments. NAT may rewrite this port.
/// Normal collection uses an OS-selected ephemeral port and TCP_NODELAY.
pub async fn connect(
    request: Request,
    source_port: Option<u16>,
) -> Result<(Socket, ConnectionInfo)> {
    let started = Instant::now();
    let uri = request.uri();
    let host = uri.host().context("WebSocket URL has no host")?;
    ensure!(
        matches!(uri.scheme_str(), Some("ws" | "wss")),
        "expected ws:// or wss:// URL"
    );
    let port = uri
        .port_u16()
        .unwrap_or(if uri.scheme_str() == Some("wss") {
            443
        } else {
            80
        });
    let addresses: Vec<_> = lookup_host((host, port)).await?.collect();
    ensure!(
        !addresses.is_empty(),
        "WebSocket hostname resolved to no addresses"
    );
    let mut last_error = None;
    for peer in addresses {
        let attempt = async {
            let socket = if peer.is_ipv4() {
                TcpSocket::new_v4()?
            } else {
                TcpSocket::new_v6()?
            };
            if let Some(port) = source_port {
                socket.set_reuseaddr(true)?;
                let ip = if peer.is_ipv4() {
                    IpAddr::V4(Ipv4Addr::UNSPECIFIED)
                } else {
                    IpAddr::V6(Ipv6Addr::UNSPECIFIED)
                };
                socket.bind(SocketAddr::new(ip, port))?;
            }
            let stream = socket.connect(peer).await?;
            stream.set_nodelay(true)?;
            Ok::<_, std::io::Error>(stream)
        }
        .await;
        let stream = match attempt {
            Ok(stream) => stream,
            Err(error) => {
                last_error = Some(error);
                continue;
            }
        };
        let local_addr = stream.local_addr()?;
        let peer_addr = stream.peer_addr()?;
        // TLS and hostname verification use the original URI, even when binding TCP manually.
        let (socket, _) = client_async_tls_with_config(request, stream, None, None).await?;
        return Ok((
            socket,
            ConnectionInfo {
                local_addr,
                peer_addr,
                requested_port: source_port,
                handshake_us: started.elapsed().as_micros().try_into().unwrap_or(u64::MAX),
            },
        ));
    }
    Err(last_error.context("no usable WebSocket address")?.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    #[tokio::test]
    async fn binds_requested_source_port_and_preserves_plain_websocket_handshake() {
        let reserved = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let requested = reserved.local_addr().unwrap().port();
        drop(reserved);
        let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = server.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (stream, peer) = server.accept().await.unwrap();
            let _socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            peer
        });
        let request = format!("ws://{address}/feed")
            .into_client_request()
            .unwrap();
        let (_socket, info) = connect(request, Some(requested)).await.unwrap();
        assert_eq!(info.local_addr.port(), requested);
        assert_eq!(info.peer_addr, address);
        assert_eq!(task.await.unwrap().port(), requested);
    }
}
