//! Server side — accept one agent's uplink and re-inject it locally.
//!
//! The room decodes each frame back into a [`ServerMessage`] and pushes it into
//! the exact channels `task_broadcaster` would have used on a driver's PC. From
//! `WebSocketServer` downwards nothing can tell the difference, which is why
//! relaying telemetry needed no protocol change.
//!
//! One room, one active source: a second agent takes over and the previous one
//! is dropped. Teams rotate drivers by simply starting an agent.
//!
//! The room also carries viewer commands the other way. [`RelayRoom::request`]
//! parks a `oneshot` under a fresh `req`, sends the command down to the agent
//! and waits for the matching `Response` — that map is what keeps N viewers'
//! answers from crossing. Engineer commands go down as `Broadcast` instead:
//! they have no single answer, so there is nothing to correlate.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

use crate::protocol::messages::{ClientCommand, ServerMessage};
use crate::relay::envelope::{DownlinkFrame, UplinkFrame};
use crate::websocket::server::WebSocketServer;

/// How deep the queue to the agent may get before a send blocks.
const DOWNLINK_CAPACITY: usize = 64;

/// Why a relayed command produced no answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestError {
    /// No agent is streaming, or the one that was went away mid-request.
    NoAgent,
    /// The agent is connected but did not answer in time.
    Timeout,
}

/// The command channel to the current agent, plus who is waiting for answers.
///
/// `std::sync::Mutex` on purpose — every critical section is one map or option
/// operation with no `.await` inside, so an async mutex would buy nothing.
///
/// Kept separate from [`RelayRoom`] so it can be constructed and tested on its
/// own, without a `WebSocketServer`.
#[derive(Default)]
struct Downlink {
    next_req: AtomicU64,
    pending: Mutex<HashMap<u64, oneshot::Sender<ServerMessage>>>,
    sender: Mutex<Option<mpsc::Sender<DownlinkFrame>>>,
    /// Last `EngineerUpdateBehavior` any viewer sent, replayed to every agent
    /// that connects. The rule engine holds one global behaviour, so without
    /// this a reconnect or a driver swap silently reverts it to defaults.
    behavior: Mutex<Option<ClientCommand>>,
}

impl Downlink {
    /// Send `cmd` to the agent and wait for its answer.
    async fn request(
        &self,
        cmd: ClientCommand,
        timeout: Duration,
    ) -> Result<ServerMessage, RequestError> {
        let tx = self
            .sender
            .lock()
            .unwrap()
            .clone()
            .ok_or(RequestError::NoAgent)?;

        let req = self.next_req.fetch_add(1, Ordering::Relaxed);
        let (res_tx, res_rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(req, res_tx);

        if tx.send(DownlinkFrame::Command { req, cmd }).await.is_err() {
            self.forget(req);
            return Err(RequestError::NoAgent);
        }

        match tokio::time::timeout(timeout, res_rx).await {
            Ok(Ok(msg)) => Ok(msg),
            // The sender was dropped: the agent disconnected under us.
            Ok(Err(_)) => Err(RequestError::NoAgent),
            Err(_) => {
                // Drop the entry here or a slow agent leaks one per timeout.
                self.forget(req);
                Err(RequestError::Timeout)
            }
        }
    }

    /// Send `cmd` to the agent without waiting: its result reaches viewers as
    /// an ordinary broadcast.
    async fn broadcast(&self, cmd: ClientCommand) -> Result<(), RequestError> {
        if matches!(cmd, ClientCommand::EngineerUpdateBehavior { .. }) {
            *self.behavior.lock().unwrap() = Some(cmd.clone());
        }

        let tx = self
            .sender
            .lock()
            .unwrap()
            .clone()
            .ok_or(RequestError::NoAgent)?;

        tx.send(DownlinkFrame::Broadcast { cmd })
            .await
            .map_err(|_| RequestError::NoAgent)
    }

    /// Re-send the stored behaviour to an agent that just said hello.
    async fn replay_behavior(&self) {
        let stored = self.behavior.lock().unwrap().clone();
        if let Some(cmd) = stored {
            let _ = self.broadcast(cmd).await;
        }
    }

    /// Hand one answer to the viewer that asked for it.
    fn resolve(&self, req: u64, msg: ServerMessage) {
        match self.pending.lock().unwrap().remove(&req) {
            Some(tx) => {
                let _ = tx.send(msg);
            }
            None => debug!("Response for unknown or timed-out request {}", req),
        }
    }

    fn forget(&self, req: u64) {
        self.pending.lock().unwrap().remove(&req);
    }

    /// Install the channel to a newly connected agent.
    fn attach(&self, tx: mpsc::Sender<DownlinkFrame>) {
        *self.sender.lock().unwrap() = Some(tx);
    }

    /// Drop the channel and wake every waiter. Dropping the `oneshot` senders
    /// is what does the waking: `request` reads that as `NoAgent` and answers
    /// immediately instead of leaving a viewer to time out against a dead agent.
    ///
    /// `behavior` deliberately survives: replaying it to the next agent is the
    /// whole point of storing it.
    fn detach(&self) {
        *self.sender.lock().unwrap() = None;
        self.pending.lock().unwrap().clear();
    }
}

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
    downlink: Downlink,
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
            downlink: Downlink::default(),
        }
    }

    /// Run one viewer command on the agent's PC and return its answer.
    pub async fn request(
        &self,
        cmd: ClientCommand,
        timeout: Duration,
    ) -> Result<ServerMessage, RequestError> {
        self.downlink.request(cmd, timeout).await
    }

    /// Run one viewer command on the agent's PC, answer to everyone.
    pub async fn broadcast(&self, cmd: ClientCommand) -> Result<(), RequestError> {
        self.downlink.broadcast(cmd).await
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

        // The sink now belongs to a writer task: command answers arrive out of
        // order, so writes have to be funnelled through one owner.
        let (mut sink, mut incoming) = ws_stream.split();
        let (out_tx, mut out_rx) = mpsc::channel::<DownlinkFrame>(DOWNLINK_CAPACITY);
        self.downlink.attach(out_tx);

        tokio::spawn(async move {
            while let Some(frame) = out_rx.recv().await {
                let bytes = match rmp_serde::to_vec_named(&frame) {
                    Ok(b) => b,
                    Err(e) => {
                        warn!("Undeliverable downlink frame: {}", e);
                        continue;
                    }
                };
                if sink.send(Message::Binary(bytes.into())).await.is_err() {
                    break;
                }
            }
        });

        loop {
            tokio::select! {
                // A newer agent claimed the room — step aside.
                changed = gen_rx.changed() => {
                    if changed.is_err() || *gen_rx.borrow() != my_generation {
                        info!("Agent {} superseded by a newer agent", peer);
                        break;
                    }
                }

                frame = incoming.next() => {
                    match frame {
                        Some(Ok(Message::Binary(bytes))) => {
                            match rmp_serde::from_slice::<UplinkFrame>(&bytes) {
                                Ok(UplinkFrame::Hello { agent_version }) => {
                                    info!("Agent {} says hello (v{})", peer, agent_version);
                                    self.downlink.replay_behavior().await;
                                }
                                Ok(UplinkFrame::Message(msg)) => self.inject(msg),
                                Ok(UplinkFrame::Response { req, msg }) => {
                                    self.downlink.resolve(req, msg);
                                }
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

        // Only the current agent's departure means the room has gone dark. A
        // superseded agent must not tear down the replacement's downlink.
        if self.generation.load(Ordering::SeqCst) == my_generation {
            info!("Agent {} disconnected — room has no source", peer);
            self.downlink.detach();
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

    fn error_stub(message: &str) -> ServerMessage {
        ServerMessage::PostRaceError {
            message: message.to_string(),
        }
    }

    fn message_of(msg: ServerMessage) -> String {
        match msg {
            ServerMessage::PostRaceError { message } => message,
            other => panic!("unexpected answer: {:?}", other),
        }
    }

    /// With no agent streaming there is nothing to wait for, so the viewer must
    /// be told at once rather than sitting out the full timeout.
    #[tokio::test]
    async fn request_without_an_agent_fails_immediately() {
        let downlink = Downlink::default();
        let err = downlink
            .request(ClientCommand::PostRaceFunFacts, Duration::from_secs(30))
            .await
            .unwrap_err();
        assert_eq!(err, RequestError::NoAgent);
    }

    /// The whole correctness argument for N viewers sharing one agent: two
    /// requests answered out of order must still reach the right waiter.
    #[tokio::test]
    async fn interleaved_requests_do_not_cross() {
        let downlink = Arc::new(Downlink::default());
        let (tx, mut rx) = mpsc::channel(8);
        downlink.attach(tx);

        let a = tokio::spawn({
            let d = downlink.clone();
            async move {
                d.request(ClientCommand::PostRaceFunFacts, Duration::from_secs(5))
                    .await
            }
        });
        let first = match rx.recv().await.unwrap() {
            DownlinkFrame::Command { req, .. } => req,
            other => panic!("unexpected frame: {:?}", other),
        };

        let b = tokio::spawn({
            let d = downlink.clone();
            async move {
                d.request(ClientCommand::PostRaceFunFacts, Duration::from_secs(5))
                    .await
            }
        });
        let second = match rx.recv().await.unwrap() {
            DownlinkFrame::Command { req, .. } => req,
            other => panic!("unexpected frame: {:?}", other),
        };
        assert_ne!(first, second);

        // Answer the second request first.
        downlink.resolve(second, error_stub("second"));
        downlink.resolve(first, error_stub("first"));

        assert_eq!(message_of(a.await.unwrap().unwrap()), "first");
        assert_eq!(message_of(b.await.unwrap().unwrap()), "second");
    }

    /// A timed-out request must not leave its entry behind, or a slow agent
    /// leaks one per abandoned request.
    #[tokio::test]
    async fn a_timeout_removes_its_pending_entry() {
        let downlink = Downlink::default();
        let (tx, _rx) = mpsc::channel(8);
        downlink.attach(tx);

        let err = downlink
            .request(ClientCommand::PostRaceFunFacts, Duration::from_millis(20))
            .await
            .unwrap_err();
        assert_eq!(err, RequestError::Timeout);
        assert!(downlink.pending.lock().unwrap().is_empty());
    }

    /// When the agent provably goes away, waiters hear about it now rather than
    /// after 30 seconds.
    #[tokio::test]
    async fn detaching_wakes_every_waiter() {
        let downlink = Arc::new(Downlink::default());
        let (tx, mut rx) = mpsc::channel(8);
        downlink.attach(tx);

        let waiter = tokio::spawn({
            let d = downlink.clone();
            async move {
                d.request(ClientCommand::PostRaceFunFacts, Duration::from_secs(30))
                    .await
            }
        });
        rx.recv().await.unwrap();

        downlink.detach();
        assert_eq!(waiter.await.unwrap().unwrap_err(), RequestError::NoAgent);
    }

    fn behavior_stub(enabled: bool) -> ClientCommand {
        ClientCommand::EngineerUpdateBehavior {
            enabled,
            frequency: "normal".to_string(),
            mute_in_qualifying: false,
            debug_all_rules_in_practice: false,
            active_voice_id: None,
            pilot_name: None,
            mute_name: false,
        }
    }

    /// The rule engine holds one global behaviour, so an agent restart would
    /// otherwise revert every setting to defaults with nobody to notice.
    #[tokio::test]
    async fn a_reconnecting_agent_is_sent_the_stored_behavior() {
        let downlink = Downlink::default();
        let (tx, mut rx) = mpsc::channel(8);
        downlink.attach(tx);

        downlink.broadcast(behavior_stub(true)).await.unwrap();
        rx.recv().await.unwrap();

        // The agent goes away and a new one connects.
        downlink.detach();
        let (tx, mut rx) = mpsc::channel(8);
        downlink.attach(tx);
        downlink.replay_behavior().await;

        match rx.recv().await.unwrap() {
            DownlinkFrame::Broadcast {
                cmd: ClientCommand::EngineerUpdateBehavior { enabled, .. },
            } => assert!(enabled),
            other => panic!("unexpected frame: {:?}", other),
        }
    }

    /// Nothing to broadcast to and nothing to wait for, so the viewer must be
    /// told rather than left thinking the setting applied.
    #[tokio::test]
    async fn broadcast_without_an_agent_fails() {
        let downlink = Downlink::default();
        let err = downlink
            .broadcast(ClientCommand::EngineerGetStatus)
            .await
            .unwrap_err();
        assert_eq!(err, RequestError::NoAgent);
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
