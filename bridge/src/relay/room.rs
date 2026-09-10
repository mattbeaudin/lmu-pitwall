//! Server side — accept one agent's uplink and re-inject it locally.
//!
//! The room decodes each frame back into a [`ServerMessage`] and pushes it into
//! the exact channels `task_broadcaster` would have used on a driver's PC. From
//! `WebSocketServer` downwards nothing can tell the difference, which is why
//! relaying telemetry needed no protocol change.
//!
//! One room, one active source: a second agent takes over and the previous one
//! is dropped. Teams rotate drivers by simply starting an agent.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures_util::StreamExt;
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

use crate::protocol::messages::ServerMessage;
use crate::relay::envelope::UplinkFrame;
use crate::websocket::server::WebSocketServer;

pub struct RelayRoom {
    agent_key: String,
    ws: Arc<WebSocketServer>,
    /// Bumped on every accepted agent. The previous agent's task sees the
    /// change on `gen_rx` and exits, closing its socket.
    generation: AtomicU64,
    gen_tx: watch::Sender<u64>,
    /// Fed from the uplink so a viewer joining mid-session gets current state
    /// instead of an empty dashboard.
    all_drivers_tx: watch::Sender<Option<ServerMessage>>,
    connection_status_tx: watch::Sender<Option<ServerMessage>>,
}

impl RelayRoom {
    pub fn new(
        agent_key: String,
        ws: Arc<WebSocketServer>,
        all_drivers_tx: watch::Sender<Option<ServerMessage>>,
        connection_status_tx: watch::Sender<Option<ServerMessage>>,
    ) -> Self {
        let (gen_tx, _) = watch::channel(0u64);
        RelayRoom {
            agent_key,
            ws,
            generation: AtomicU64::new(0),
            gen_tx,
            all_drivers_tx,
            connection_status_tx,
        }
    }

    /// Take over an already-accepted TCP connection on `/uplink`.
    pub fn accept_agent(self: &Arc<Self>, stream: TcpStream, peer: SocketAddr) {
        let room = self.clone();
        tokio::spawn(async move { room.handle_agent(stream, peer).await });
    }

    async fn handle_agent(self: Arc<Self>, stream: TcpStream, peer: SocketAddr) {
        let expected = self.agent_key.clone();
        let ws_stream = match tokio_tungstenite::accept_hdr_async(
            stream,
            move |req: &Request, resp: Response| {
                let presented = req
                    .headers()
                    .get("Authorization")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.strip_prefix("Bearer "))
                    .unwrap_or("");

                if keys_match(presented, &expected) {
                    Ok(resp)
                } else {
                    let mut err = ErrorResponse::new(Some("invalid agent key".to_string()));
                    *err.status_mut() = tokio_tungstenite::tungstenite::http::StatusCode::UNAUTHORIZED;
                    Err(err)
                }
            },
        )
        .await
        {
            Ok(s) => s,
            Err(e) => {
                warn!("Agent handshake rejected for {}: {}", peer, e);
                return;
            }
        };

        // Claim the room. Any agent already streaming sees this and exits.
        let my_generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = self.gen_tx.send(my_generation);
        let mut gen_rx = self.gen_tx.subscribe();
        info!("Agent {} connected (generation {})", peer, my_generation);

        let (_sink, mut incoming) = ws_stream.split();

        loop {
            tokio::select! {
                // A newer agent claimed the room — step aside.
                changed = gen_rx.changed() => {
                    if changed.is_err() || *gen_rx.borrow() != my_generation {
                        info!("Agent {} superseded by a newer agent", peer);
                        return;
                    }
                }

                frame = incoming.next() => {
                    match frame {
                        Some(Ok(Message::Binary(bytes))) => {
                            match rmp_serde::from_slice::<UplinkFrame>(&bytes) {
                                Ok(UplinkFrame::Hello { agent_version }) => {
                                    info!("Agent {} says hello (v{})", peer, agent_version);
                                }
                                Ok(UplinkFrame::Message(msg)) => self.inject(msg),
                                Err(e) => warn!("Undecodable frame from agent {}: {}", peer, e),
                            }
                        }
                        Some(Ok(Message::Close(_))) | None => {
                            debug!("Agent {} closed the uplink", peer);
                            break;
                        }
                        Some(Err(e)) => {
                            debug!("Agent {} connection error: {}", peer, e);
                            break;
                        }
                        Some(Ok(_)) => {
                            // Text, ping and pong frames are not part of the uplink.
                        }
                    }
                }
            }
        }

        // Only the current agent's departure means the room has gone dark.
        if self.generation.load(Ordering::SeqCst) == my_generation {
            info!("Agent {} disconnected — room has no source", peer);
            self.inject(ServerMessage::ConnectionStatus {
                game_connected: false,
                plugin_version: String::new(),
                telemetry_valid: false,
                telemetry_warning: Some("Agent disconnected".to_string()),
            });
        }
    }

    /// Push one relayed message into the local broadcast fabric.
    fn inject(&self, msg: ServerMessage) {
        // A late-joining viewer is served from the watch channels, so they must
        // be fed here too — otherwise everyone who connects mid-session sees an
        // empty dashboard until the next car crosses the line.
        match &msg {
            ServerMessage::AllDriversUpdate { .. } => {
                let _ = self.all_drivers_tx.send(Some(msg.clone()));
            }
            ServerMessage::ConnectionStatus { .. } => {
                let _ = self.connection_status_tx.send(Some(msg.clone()));
            }
            _ => {}
        }

        match route_of(&msg) {
            Route::Viewers => {
                let _ = self.ws.broadcaster().send(Arc::new(msg));
            }
            Route::Audio => {
                let _ = self.ws.audio_broadcaster().send(Arc::new(msg));
            }
            Route::Drop => {}
        }
    }
}

/// Which channel a relayed message belongs on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Route {
    Viewers,
    Audio,
    Drop,
}

fn route_of(msg: &ServerMessage) -> Route {
    match msg {
        // Must stay on the audio channel, or viewers registered `display_only`
        // receive every WAV payload the engineer produces.
        ServerMessage::EngineerAudio { .. } => Route::Audio,
        // The relay runs its own update check. Its version is what a viewer
        // needs to see — the agent's would point them at the wrong download.
        ServerMessage::VersionInfo { .. } => Route::Drop,
        _ => Route::Viewers,
    }
}

/// Constant-time key comparison.
///
/// Length is allowed to leak — it is not the secret. Content is not: a
/// short-circuiting `==` on a public endpoint leaks the key one byte at a time.
fn keys_match(presented: &str, expected: &str) -> bool {
    if presented.len() != expected.len() {
        return false;
    }
    let mut diff = 0u8;
    for (a, b) in presented.bytes().zip(expected.bytes()) {
        diff |= a ^ b;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_comparison_accepts_only_the_exact_key() {
        assert!(keys_match("s3cret", "s3cret"));
        assert!(!keys_match("s3cret", "s3crev"));
        assert!(!keys_match("s3cre", "s3cret"));
        assert!(!keys_match("", "s3cret"));
        assert!(!keys_match("s3cret", ""));
    }

    /// An empty configured key must not turn the uplink into an open endpoint;
    /// `main` refuses to start a server without one, and this is the backstop.
    #[test]
    fn empty_expected_key_still_rejects_a_presented_key() {
        assert!(!keys_match("anything", ""));
    }

    /// Audio must not leak onto the main channel: a `display_only` viewer that
    /// starts receiving WAV payloads is the failure this guards against.
    #[test]
    fn engineer_audio_stays_on_the_audio_channel() {
        let audio = ServerMessage::EngineerAudio {
            request_id: String::new(),
            priority: "normal".to_string(),
            wav_base64: String::new(),
            sample_rate: 22_050,
            duration_ms: 0,
            text: String::new(),
        };
        assert_eq!(route_of(&audio), Route::Audio);
    }

    #[test]
    fn agent_version_info_is_not_relayed_to_viewers() {
        let version = ServerMessage::VersionInfo {
            current_version: "1.0.0".to_string(),
            latest_version: "1.0.0".to_string(),
            download_url: String::new(),
            update_available: false,
        };
        assert_eq!(route_of(&version), Route::Drop);
    }

    #[test]
    fn ordinary_telemetry_goes_to_every_viewer() {
        let status = ServerMessage::ConnectionStatus {
            game_connected: true,
            plugin_version: String::new(),
            telemetry_valid: true,
            telemetry_warning: None,
        };
        assert_eq!(route_of(&status), Route::Viewers);
    }
}
