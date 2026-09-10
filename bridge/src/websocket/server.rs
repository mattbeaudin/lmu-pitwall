use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, watch};
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

use crate::fuel_calculator::api::handle_command as fuel_handle;
use crate::post_race::api::handle_command as post_race_handle;
use crate::protocol::messages::{ClientCommand, ServerMessage};
use crate::race_engineer::{self, RaceEngineerService};
use crate::relay::room::{RelayRoom, RequestError};

/// Broadcast channel capacity — number of queued messages per slow client
/// before older messages are dropped (lagged receiver).
const BROADCAST_CAPACITY: usize = 128;

/// How many viewer sockets may be connected at once.
///
/// Every viewer holds a subscription to both broadcast channels and a socket
/// buffer. On a public relay the URL is the only thing needed to open one, so
/// the ceiling is explicit rather than however many the OS will allow. The
/// agent's uplink is not a viewer and is not counted — it never reaches here.
const MAX_CLIENTS: usize = 20;

/// Wire serialization format negotiated per-client via query parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// MessagePack binary — primary format, compact and fast.
    MsgPack,
    /// JSON text — enabled via `?format=json`, useful for debugging.
    Json,
}

/// WebSocket broadcast server.
///
/// Accepts clients on `ws://0.0.0.0:<port>` and fans out every
/// [`ServerMessage`] sent via [`broadcast`] to all connected clients.
/// `EngineerAudio` messages use a separate `audio_tx` channel so that clients
/// registered as `display_only` never receive large WAV payloads.
pub struct WebSocketServer {
    port: u16,
    tx: broadcast::Sender<Arc<ServerMessage>>,
    /// Audio-only channel — only `EngineerAudio` messages are sent here.
    audio_tx: broadcast::Sender<Arc<ServerMessage>>,
    /// Live count of connected WebSocket clients.
    client_count: Arc<AtomicUsize>,
    /// Watch channel — subscribers receive the new count on every change.
    count_tx: watch::Sender<usize>,
    /// Latest AllDriversUpdate — sent immediately to newly connecting clients.
    all_drivers_rx: watch::Receiver<Option<ServerMessage>>,
    /// Latest VersionInfo — sent immediately to newly connecting clients.
    version_info_rx: watch::Receiver<Option<ServerMessage>>,
    /// Latest ConnectionStatus — sent immediately to newly connecting clients.
    connection_status_rx: watch::Receiver<Option<ServerMessage>>,
    engineer_service: Arc<RaceEngineerService>,
}

impl WebSocketServer {
    pub fn new(
        port: u16,
        all_drivers_rx: watch::Receiver<Option<ServerMessage>>,
        version_info_rx: watch::Receiver<Option<ServerMessage>>,
        connection_status_rx: watch::Receiver<Option<ServerMessage>>,
        engineer_service: Arc<RaceEngineerService>,
    ) -> Self {
        let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let (audio_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let (count_tx, _) = watch::channel(0usize);
        WebSocketServer {
            port,
            tx,
            audio_tx,
            client_count: Arc::new(AtomicUsize::new(0)),
            count_tx,
            all_drivers_rx,
            version_info_rx,
            connection_status_rx,
            engineer_service,
        }
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// Subscribe to client-count changes.
    pub fn client_count_rx(&self) -> watch::Receiver<usize> {
        self.count_tx.subscribe()
    }

    /// Returns a cloned sender for broadcasting from another task.
    pub fn broadcaster(&self) -> broadcast::Sender<Arc<ServerMessage>> {
        self.tx.clone()
    }

    /// Returns a cloned sender for audio-only broadcast (EngineerAudio messages).
    /// Only clients registered as `audio` role receive from this channel.
    pub fn audio_broadcaster(&self) -> broadcast::Sender<Arc<ServerMessage>> {
        self.audio_tx.clone()
    }

    /// The engineer service this process owns. The relay uplink needs it to run
    /// engineer commands the server sent down.
    pub fn engineer_service(&self) -> Arc<RaceEngineerService> {
        self.engineer_service.clone()
    }

    /// Send a message to every connected client.
    pub fn broadcast(&self, msg: ServerMessage) -> usize {
        match self.tx.send(Arc::new(msg)) {
            Ok(n) => n,
            Err(_) => 0,
        }
    }

    /// Accept a single WebSocket client from an already-accepted `TcpStream`.
    ///
    /// `relay` is `Some` only in server mode, and is passed per connection
    /// rather than stored on the server: the room is built *from* an
    /// `Arc<WebSocketServer>`, so holding one here would be a cycle.
    pub fn accept_client(
        &self,
        stream: TcpStream,
        peer: SocketAddr,
        relay: Option<Arc<RelayRoom>>,
    ) {
        info!("New WebSocket connection from {}", peer);

        // Claim a slot atomically: two connections arriving together must not
        // both read the same count and both decide there is room.
        let claimed = self
            .client_count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                (n < MAX_CLIENTS).then_some(n + 1)
            });

        let count = match claimed {
            Ok(prev) => prev + 1,
            Err(n) => {
                warn!("Refusing {} — already at {} viewers", peer, n);
                tokio::spawn(refuse_client(stream));
                return;
            }
        };
        let _ = self.count_tx.send(count);

        let rx = self.tx.subscribe();
        let audio_rx = self.audio_tx.subscribe();

        tokio::spawn(handle_client(
            stream,
            peer,
            rx,
            audio_rx,
            self.client_count.clone(),
            self.count_tx.clone(),
            self.all_drivers_rx.clone(),
            self.version_info_rx.clone(),
            self.connection_status_rx.clone(),
            self.tx.clone(),
            self.audio_tx.clone(),
            self.engineer_service.clone(),
            relay,
        ));
    }
}

// ---------------------------------------------------------------------------
// Per-client handler
// ---------------------------------------------------------------------------

async fn handle_client(
    stream: TcpStream,
    peer: SocketAddr,
    mut rx: broadcast::Receiver<Arc<ServerMessage>>,
    mut audio_rx: broadcast::Receiver<Arc<ServerMessage>>,
    client_count: Arc<AtomicUsize>,
    count_tx: watch::Sender<usize>,
    all_drivers_rx: watch::Receiver<Option<ServerMessage>>,
    version_info_rx: watch::Receiver<Option<ServerMessage>>,
    connection_status_rx: watch::Receiver<Option<ServerMessage>>,
    ws_broadcaster: broadcast::Sender<Arc<ServerMessage>>,
    _audio_broadcaster: broadcast::Sender<Arc<ServerMessage>>,
    engineer_service: Arc<RaceEngineerService>,
    relay: Option<Arc<RelayRoom>>,
) {
    // Released on every exit path, including a failed handshake below.
    let _slot = ClientSlot {
        count: client_count,
        count_tx,
    };

    // Default role is audio — this device plays engineer callouts.
    let mut is_audio = true;
    let mut command_rate = CommandRate::new();
    let is_json = Arc::new(AtomicBool::new(false));
    let is_json_cb = is_json.clone();

    let ws =
        match tokio_tungstenite::accept_hdr_async_with_config(
            stream,
            move |req: &Request, resp: Response| {
                let json_requested = req
                    .uri()
                    .query()
                    .map(|q| q.split('&').any(|p| p == "format=json"))
                    .unwrap_or(false);
                if json_requested {
                    is_json_cb.store(true, Ordering::Relaxed);
                }
                Ok(resp)
            },
            Some(client_ws_config()),
        )
        .await
        {
            Ok(ws) => ws,
            Err(e) => {
                warn!("WebSocket handshake failed for {}: {}", peer, e);
                return;
            }
        };

    let fmt = if is_json.load(Ordering::Relaxed) {
        Format::Json
    } else {
        Format::MsgPack
    };
    info!("Client {} connected (format: {:?})", peer, fmt);

    let (mut sink, mut incoming) = ws.split();

    // Send the latest AllDriversUpdate snapshot immediately on connect so the
    // frontend doesn't wait until the next car crosses the S/F line.
    {
        let latest = all_drivers_rx.borrow().clone();
        if let Some(msg) = latest {
            if let Ok(ws_msg) = serialize(&msg, fmt) {
                let _ = sink.send(ws_msg).await;
            }
        }
    }

    // Send the latest VersionInfo immediately on connect (if check has completed).
    {
        let latest = version_info_rx.borrow().clone();
        if let Some(msg) = latest {
            if let Ok(ws_msg) = serialize(&msg, fmt) {
                let _ = sink.send(ws_msg).await;
            }
        }
    }

    // Send the latest ConnectionStatus immediately on connect so the frontend
    // never shows "Waiting" when the game was already running before connect.
    {
        let latest = connection_status_rx.borrow().clone();
        if let Some(msg) = latest {
            if let Ok(ws_msg) = serialize(&msg, fmt) {
                let _ = sink.send(ws_msg).await;
            }
        }
    }

    loop {
        tokio::select! {
            // Outbound: receive broadcast message and forward to this client.
            result = rx.recv() => {
                match result {
                    Ok(msg) => {
                        match serialize(&msg, fmt) {
                            Ok(ws_msg) => {
                                if let Err(e) = sink.send(ws_msg).await {
                                    debug!("Send to {} failed: {}", peer, e);
                                    break;
                                }
                            }
                            Err(e) => {
                                warn!("Serialization error for {}: {}", peer, e);
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(dropped)) => {
                        warn!(
                            "Client {} is too slow, dropped {} messages",
                            peer, dropped
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }

            // Audio-only channel: only forwarded to clients with `audio` role.
            audio_result = audio_rx.recv() => {
                if is_audio {
                    match audio_result {
                        Ok(msg) => {
                            match serialize(&msg, fmt) {
                                Ok(ws_msg) => {
                                    if let Err(e) = sink.send(ws_msg).await {
                                        debug!("Send audio to {} failed: {}", peer, e);
                                        break;
                                    }
                                }
                                Err(e) => {
                                    warn!("Audio serialization error for {}: {}", peer, e);
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(dropped)) => {
                            warn!("Client {} audio too slow, dropped {}", peer, dropped);
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            break;
                        }
                    }
                } else {
                    // display_only: drain audio_rx without forwarding.
                    if let Err(broadcast::error::RecvError::Closed) = audio_result {
                        break;
                    }
                }
            }

            // Inbound: dispatch text commands; close and drop everything else.
            frame = incoming.next() => {
                match frame {
                    Some(Ok(Message::Close(_))) | None => {
                        debug!("Client {} sent close", peer);
                        break;
                    }
                    Some(Err(e)) => {
                        debug!("Client {} connection error: {}", peer, e);
                        break;
                    }
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<ClientCommand>(&text) {
                            Ok(cmd) => {
                                // Per-client role registration — handled locally.
                                if let ClientCommand::EngineerRegisterClientRole { ref role } = cmd {
                                    is_audio = role != "display_only";
                                    info!(
                                        "Client {} registered as {} (is_audio={})",
                                        peer, role, is_audio
                                    );
                                    continue;
                                }

                                if is_engineer(&cmd) {
                                    // In server mode the engineer — Piper, the
                                    // voices and the rule engine — lives on the
                                    // agent's PC, so the command goes down the
                                    // relay and its result comes back as an
                                    // ordinary broadcast.
                                    let refusal = match &relay {
                                        Some(room) => {
                                            relay_engineer(room, &mut command_rate, cmd).await
                                        }
                                        None => {
                                            let svc = engineer_service.clone();
                                            let bcast = ws_broadcaster.clone();
                                            let audio_bcast = _audio_broadcaster.clone();
                                            tokio::spawn(async move {
                                                race_engineer::api::handle_command(cmd, svc, bcast, audio_bcast).await;
                                            });
                                            None
                                        }
                                    };

                                    // Only this socket asked, so only this
                                    // socket hears why it did not work.
                                    if let Some(msg) = refusal {
                                        match serialize(&msg, fmt) {
                                            Ok(ws_msg) => {
                                                if let Err(e) = sink.send(ws_msg).await {
                                                    debug!("Send to {} failed: {}", peer, e);
                                                    break;
                                                }
                                            }
                                            Err(e) => {
                                                warn!("Serialization error for {}: {}", peer, e);
                                            }
                                        }
                                    }
                                } else {
                                    // In server mode the data lives on the agent's PC,
                                    // so the command makes a round trip. `relay` is
                                    // None locally and this path is unchanged.
                                    let msg = match &relay {
                                        Some(room) if is_relayable(&cmd) => {
                                            relay_command(room, &mut command_rate, cmd).await
                                        }
                                        _ => execute_local(cmd).await,
                                    };

                                    match serialize(&msg, fmt) {
                                        Ok(ws_msg) => {
                                            if let Err(e) = sink.send(ws_msg).await {
                                                debug!("Send to {} failed: {}", peer, e);
                                                break;
                                            }
                                        }
                                        Err(e) => {
                                            warn!("Serialization error for {}: {}", peer, e);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Unparseable client command from {}: {}", peer, e);
                            }
                        }
                    }
                    Some(Ok(_)) => {
                        // Binary and other frames ignored.
                    }
                }
            }
        }
    }

    info!("Client {} disconnected", peer);
}

/// Holds one of the [`MAX_CLIENTS`] slots for as long as a client is connected.
///
/// A guard rather than a `fetch_sub` at the end of `handle_client`: the
/// handshake can fail and return early, and with a hard cap a slot that is
/// never given back is not a wrong number on a status line — it is one fewer
/// viewer the server will ever accept again.
struct ClientSlot {
    count: Arc<AtomicUsize>,
    count_tx: watch::Sender<usize>,
}

impl Drop for ClientSlot {
    fn drop(&mut self) {
        let prev = self.count.fetch_sub(1, Ordering::Relaxed);
        let _ = self.count_tx.send(prev.saturating_sub(1));
    }
}

/// Turn away a viewer that arrived when the server was full.
///
/// The WebSocket handshake is an HTTP request and none of it has happened yet,
/// so a plain 503 is both legal and the only way the browser learns why rather
/// than seeing a bare disconnect.
async fn refuse_client(mut stream: TcpStream) {
    let body = "Too many viewers connected";
    let response = format!(
        "HTTP/1.1 503 Service Unavailable\r\n\
         Content-Type: text/plain\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;
}

/// Limits for a viewer socket.
///
/// Commands are small JSON objects. tungstenite's default ceiling is 64 MiB
/// per message, which on a public relay is 64 MiB a stranger can make the
/// server hold for the price of one connection.
fn client_ws_config() -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
    let mut config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default();
    config.max_message_size = Some(256 * 1024);
    config.max_frame_size = Some(256 * 1024);
    config
}

// ---------------------------------------------------------------------------
// Command dispatcher
// ---------------------------------------------------------------------------

/// Route an incoming [`ClientCommand`] to the appropriate module handler.
///
/// Fuel-calculator commands go to `fuel_calculator::api`, all others to
/// `post_race::api`. Both handlers are synchronous and must be called from
/// `tokio::task::spawn_blocking`.
fn dispatch_command(cmd: ClientCommand) -> ServerMessage {
    match cmd {
        ClientCommand::FuelCalcInit | ClientCommand::FuelCalcCompute { .. } => {
            fuel_handle(cmd)
        }
        _ => post_race_handle(cmd),
    }
}

/// Send one viewer command down the relay, answering with the panel's own
/// error variant when it cannot be served.
async fn relay_command(
    room: &Arc<RelayRoom>,
    rate: &mut CommandRate,
    cmd: ClientCommand,
) -> ServerMessage {
    if !rate.allow() {
        return command_error(&cmd, "Too many requests — slow down".to_string());
    }

    let timeout = command_timeout(&cmd);
    match room.request(sanitize_for_relay(cmd.clone()), timeout).await {
        Ok(msg) => msg,
        Err(RequestError::NoAgent) => {
            command_error(&cmd, "No agent connected to this relay".to_string())
        }
        Err(RequestError::Timeout) => {
            command_error(&cmd, "The agent did not answer in time".to_string())
        }
    }
}

/// Strip fields a remote viewer must not choose for the driver's machine.
///
/// `PostRaceInit` carries the folder to import from. Locally that is the
/// driver picking their own results directory; over the relay it is an
/// anonymous viewer naming any path on someone else's PC, which the importer
/// then walks recursively. The agent's own default folder is the only sensible
/// answer, so drop what the viewer sent.
fn sanitize_for_relay(cmd: ClientCommand) -> ClientCommand {
    match cmd {
        ClientCommand::PostRaceInit { results_path } => {
            if results_path.is_some() {
                warn!("Ignoring a relayed results_path — the agent uses its own folder");
            }
            ClientCommand::PostRaceInit { results_path: None }
        }
        other => other,
    }
}

/// Run a command on this machine.
///
/// Shared by the local socket path and the relay agent's downlink handler, so
/// a remote viewer gets exactly what a local one would.
pub(crate) async fn execute_local(cmd: ClientCommand) -> ServerMessage {
    tokio::task::spawn_blocking(move || dispatch_command(cmd))
        .await
        .unwrap_or_else(|e| ServerMessage::PostRaceError {
            message: format!("command task panicked: {e}"),
        })
}

/// Whether a command may be sent down the relay to the agent's PC.
///
/// Listed explicitly rather than with a catch-all: anything a later phase adds
/// must be opted in here, not forwarded to a driver's machine by accident.
pub(crate) fn is_relayable(cmd: &ClientCommand) -> bool {
    matches!(
        cmd,
        ClientCommand::PostRaceInit { .. }
            | ClientCommand::PostRaceSessionDetail { .. }
            | ClientCommand::PostRaceDriverLaps { .. }
            | ClientCommand::PostRaceCompare { .. }
            | ClientCommand::PostRaceStintSummary { .. }
            | ClientCommand::PostRaceEvents { .. }
            | ClientCommand::PostRaceFunFacts
            | ClientCommand::FuelCalcInit
            | ClientCommand::FuelCalcCompute { .. }
    )
}

/// Whether a command belongs to the Race Engineer.
///
/// Listed explicitly rather than by prefix so a later variant must be opted in
/// here. `EngineerRegisterClientRole` is absent on purpose: it is per-socket
/// state, handled in `handle_client` before this is ever consulted.
pub(crate) fn is_engineer(cmd: &ClientCommand) -> bool {
    matches!(
        cmd,
        ClientCommand::EngineerGetStatus
            | ClientCommand::EngineerInstallPiper
            | ClientCommand::EngineerInstallVoice { .. }
            | ClientCommand::EngineerUninstallVoice { .. }
            | ClientCommand::EngineerSynthesize { .. }
            | ClientCommand::EngineerUpdateBehavior { .. }
    )
}

/// Send one engineer command down the relay.
///
/// Returns `None` when it is on its way — the agent broadcasts the result, so
/// there is nothing to answer with here — and an `EngineerError` for this
/// socket alone when it could not be sent.
async fn relay_engineer(
    room: &Arc<RelayRoom>,
    rate: &mut CommandRate,
    cmd: ClientCommand,
) -> Option<ServerMessage> {
    if !rate.allow() {
        return Some(ServerMessage::EngineerError {
            message: "Too many requests — slow down".to_string(),
        });
    }

    match room.broadcast(cmd).await {
        Ok(()) => None,
        Err(_) => Some(ServerMessage::EngineerError {
            message: "No agent connected to this relay".to_string(),
        }),
    }
}

/// How long to wait for the agent's answer.
///
/// A cold `PostRaceInit` walks the results folder and parses every XML file
/// (`post_race/importer.rs`), which is far more than a query's worth of work.
pub(crate) fn command_timeout(cmd: &ClientCommand) -> Duration {
    match cmd {
        ClientCommand::PostRaceInit { .. } => Duration::from_secs(30),
        _ => Duration::from_secs(10),
    }
}

/// The error variant the panel that sent `cmd` knows how to render.
///
/// There are two, and picking the wrong one leaves the panel showing nothing.
pub(crate) fn command_error(cmd: &ClientCommand, message: String) -> ServerMessage {
    match cmd {
        ClientCommand::FuelCalcInit | ClientCommand::FuelCalcCompute { .. } => {
            ServerMessage::FuelCalcError { message }
        }
        _ => ServerMessage::PostRaceError { message },
    }
}

/// Caps how fast one viewer socket may ask the agent to do work.
///
/// These are click-driven, so a few per second is generous. Only consulted on
/// the relay path — a viewer there is anonymous, and the commands cost real
/// work on someone else's PC.
struct CommandRate {
    tokens: f64,
    last: Instant,
}

impl CommandRate {
    const PER_SEC: f64 = 5.0;

    fn new() -> Self {
        CommandRate {
            tokens: Self::PER_SEC,
            last: Instant::now(),
        }
    }

    fn allow(&mut self) -> bool {
        let now = Instant::now();
        self.tokens =
            (self.tokens + now.duration_since(self.last).as_secs_f64() * Self::PER_SEC)
                .min(Self::PER_SEC);
        self.last = now;

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

// ---------------------------------------------------------------------------
// Serialization helpers
// ---------------------------------------------------------------------------

fn serialize(msg: &ServerMessage, fmt: Format) -> Result<Message> {
    match fmt {
        Format::MsgPack => {
            let bytes = rmp_serde::to_vec_named(msg)?;
            Ok(Message::Binary(bytes.into()))
        }
        Format::Json => {
            let text = serde_json::to_string(msg)?;
            Ok(Message::Text(text.into()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Engineer commands take the `Broadcast` frame, not the `req`-correlated
    /// one, so they must never be picked up by the request path as well.
    #[test]
    fn engineer_commands_are_not_relayable() {
        assert!(!is_relayable(&ClientCommand::EngineerGetStatus));
        assert!(!is_relayable(&ClientCommand::EngineerInstallPiper));
        assert!(!is_relayable(&ClientCommand::EngineerRegisterClientRole {
            role: "audio".to_string(),
        }));
    }

    /// The relay branch keys off this, so a command missing here would run on
    /// the VPS — where there is no Piper, no voices and no rule engine.
    #[test]
    fn every_engineer_command_is_recognised() {
        assert!(is_engineer(&ClientCommand::EngineerGetStatus));
        assert!(is_engineer(&ClientCommand::EngineerInstallPiper));
        assert!(is_engineer(&ClientCommand::EngineerInstallVoice {
            voice_id: "v".to_string(),
        }));
        assert!(is_engineer(&ClientCommand::EngineerUninstallVoice {
            voice_id: "v".to_string(),
        }));
        assert!(is_engineer(&ClientCommand::EngineerSynthesize {
            voice_id: "v".to_string(),
            text: String::new(),
            request_id: String::new(),
        }));
        assert!(is_engineer(&behavior_stub()));
    }

    /// Per-socket state. Forwarding it would set the *agent's* audio role from
    /// a remote viewer's browser.
    #[test]
    fn registering_a_client_role_is_not_an_engineer_command() {
        assert!(!is_engineer(&ClientCommand::EngineerRegisterClientRole {
            role: "display_only".to_string(),
        }));
        assert!(!is_engineer(&ClientCommand::PostRaceFunFacts));
    }

    fn behavior_stub() -> ClientCommand {
        ClientCommand::EngineerUpdateBehavior {
            enabled: true,
            frequency: "normal".to_string(),
            mute_in_qualifying: false,
            debug_all_rules_in_practice: false,
            active_voice_id: None,
            pilot_name: None,
            mute_name: false,
        }
    }

    /// The importer walks this path recursively on the driver's PC. A viewer
    /// on a public URL must not get to choose it.
    #[test]
    fn a_relayed_import_cannot_name_its_own_folder() {
        let sent = ClientCommand::PostRaceInit {
            results_path: Some("/etc".to_string()),
        };
        match sanitize_for_relay(sent) {
            ClientCommand::PostRaceInit { results_path } => assert_eq!(results_path, None),
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn post_race_and_fuel_commands_are_relayable() {
        assert!(is_relayable(&ClientCommand::PostRaceFunFacts));
        assert!(is_relayable(&ClientCommand::PostRaceInit { results_path: None }));
        assert!(is_relayable(&ClientCommand::FuelCalcInit));
    }

    /// A cold import parses every result XML, so it gets far longer than a
    /// query that only reads the database.
    #[test]
    fn a_cold_import_gets_the_long_timeout() {
        assert_eq!(
            command_timeout(&ClientCommand::PostRaceInit { results_path: None }),
            Duration::from_secs(30)
        );
        assert_eq!(
            command_timeout(&ClientCommand::PostRaceFunFacts),
            Duration::from_secs(10)
        );
    }

    /// Two error variants exist and each panel renders only its own — the
    /// wrong one shows the viewer nothing at all.
    #[test]
    fn errors_go_back_in_the_shape_the_panel_expects() {
        assert!(matches!(
            command_error(&ClientCommand::FuelCalcInit, "x".to_string()),
            ServerMessage::FuelCalcError { .. }
        ));
        assert!(matches!(
            command_error(&ClientCommand::PostRaceFunFacts, "x".to_string()),
            ServerMessage::PostRaceError { .. }
        ));
    }

    /// The bucket has to refill, or a viewer who clicks quickly once is capped
    /// for the rest of the session.
    #[test]
    fn the_rate_limiter_allows_a_burst_then_refills() {
        let mut rate = CommandRate::new();
        for _ in 0..5 {
            assert!(rate.allow());
        }
        assert!(!rate.allow());

        rate.last = Instant::now() - Duration::from_secs(1);
        assert!(rate.allow());
    }
}
