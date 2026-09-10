//! Agent side — forward this PC's telemetry to a relay server.
//!
//! This is a *subscriber*, not a second data path. `task_polling` and
//! `task_broadcaster` are untouched and keep doing all the real work; the
//! uplink just reads the broadcast channels they already write to.
//!
//! Subscribing is also what keeps a headless agent alive:
//! `WebSocketServer::broadcast` drops the message when there are no receivers,
//! so with `--headless` and no browser open the pipeline would otherwise be
//! sending into a void.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::SinkExt;
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

use crate::protocol::messages::ServerMessage;
use crate::relay::envelope::UplinkFrame;
use crate::websocket::server::WebSocketServer;

/// Reconnect backoff, same shape as the frontend's (`useWebSocket.ts`).
const BASE_DELAY_MS: u64 = 1_000;
const MAX_DELAY_MS: u64 = 30_000;

/// How often the uplink logs its measured throughput.
const THROUGHPUT_LOG_SECS: u64 = 30;

/// A connection that lasted at least this long counts as healthy and resets the
/// reconnect backoff.
const STABLE_CONNECTION_SECS: u64 = 60;

/// Connect to `relay_url` and stream every broadcast message to it, forever.
///
/// While disconnected, messages are dropped rather than buffered — telemetry is
/// only useful live, and the broadcast channel is bounded anyway.
pub async fn task_uplink(
    ws: Arc<WebSocketServer>,
    relay_url: String,
    agent_key: String,
    uplink_fps: u32,
) {
    let mut attempt: u32 = 0;

    loop {
        let started = Instant::now();
        match stream_once(&ws, &relay_url, &agent_key, uplink_fps).await {
            Ok(()) => warn!("Uplink to {} closed by the relay", relay_url),
            Err(e) => warn!("Uplink to {} failed: {}", relay_url, e),
        }

        // Back off on any short-lived connection, including a clean close.
        // The relay holds one source at a time, so two agents left running
        // against the same room would otherwise take turns evicting each other
        // as fast as they could reconnect.
        if started.elapsed() >= Duration::from_secs(STABLE_CONNECTION_SECS) {
            attempt = 0;
        } else {
            attempt = attempt.saturating_add(1);
        }

        let delay = std::cmp::min(
            BASE_DELAY_MS.saturating_mul(1 << attempt.min(5)),
            MAX_DELAY_MS,
        );
        info!("Reconnecting to relay in {} ms", delay);
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }
}

/// One connection lifetime. Returns `Ok` on a clean close, `Err` on failure.
async fn stream_once(
    ws: &Arc<WebSocketServer>,
    relay_url: &str,
    agent_key: &str,
    uplink_fps: u32,
) -> anyhow::Result<()> {
    // Subscribe before connecting so no message is missed during the handshake.
    let mut rx = ws.broadcaster().subscribe();
    let mut audio_rx = ws.audio_broadcaster().subscribe();

    // The key goes in a header, not the query string — query strings end up in
    // proxy access logs.
    let mut request = relay_url.into_client_request()?;
    request.headers_mut().insert(
        "Authorization",
        format!("Bearer {}", agent_key).parse()?,
    );

    let (mut sink, _incoming) = {
        let (stream, response) = tokio_tungstenite::connect_async(request).await?;
        debug!("Relay handshake status: {}", response.status());
        futures_util::StreamExt::split(stream)
    };

    info!("Uplink connected to {}", relay_url);

    let hello = UplinkFrame::Hello {
        agent_version: env!("CARGO_PKG_VERSION").to_string(),
    };
    sink.send(Message::Binary(rmp_serde::to_vec_named(&hello)?.into()))
        .await?;

    let mut throttle = Throttle::new(uplink_fps);
    let mut meter = Meter::new();

    loop {
        let msg = tokio::select! {
            result = rx.recv() => match result {
                Ok(msg) => msg,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("Uplink lagged, dropped {} messages", n);
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => return Ok(()),
            },
            result = audio_rx.recv() => match result {
                Ok(msg) => msg,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("Uplink audio lagged, dropped {} messages", n);
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => return Ok(()),
            },
        };

        if throttle.should_drop(&msg) {
            continue;
        }

        let bytes = rmp_serde::to_vec_named(&UplinkFrame::Message((*msg).clone()))?;
        meter.record(bytes.len());
        sink.send(Message::Binary(bytes.into())).await?;
    }
}

/// Rate-limits the two high-frequency message types down to `--uplink-fps`.
///
/// Everything else (lap snapshots, engineer callouts, post-race responses) is
/// event-driven and passes through untouched.
struct Throttle {
    interval: Duration,
    last_telemetry: Option<Instant>,
    last_scoring: Option<Instant>,
}

impl Throttle {
    fn new(fps: u32) -> Self {
        Throttle {
            interval: Duration::from_secs_f64(1.0 / fps.max(1) as f64),
            last_telemetry: None,
            last_scoring: None,
        }
    }

    fn should_drop(&mut self, msg: &ServerMessage) -> bool {
        let slot = match msg {
            ServerMessage::TelemetryUpdate { .. } => &mut self.last_telemetry,
            ServerMessage::ScoringUpdate { .. } => &mut self.last_scoring,
            _ => return false,
        };

        let now = Instant::now();
        match slot {
            Some(last) if now.duration_since(*last) < self.interval => true,
            _ => {
                *slot = Some(now);
                false
            }
        }
    }
}

/// Measures actual uplink throughput.
///
/// A full 34-car grid's `ScoringUpdate` is the dominant cost and was the
/// biggest unknown when this was designed — the number this logs is what
/// `--uplink-fps` should be tuned against on a real home connection.
struct Meter {
    bytes: usize,
    frames: usize,
    window_start: Instant,
}

impl Meter {
    fn new() -> Self {
        Meter {
            bytes: 0,
            frames: 0,
            window_start: Instant::now(),
        }
    }

    fn record(&mut self, len: usize) {
        self.bytes += len;
        self.frames += 1;

        let elapsed = self.window_start.elapsed();
        if elapsed >= Duration::from_secs(THROUGHPUT_LOG_SECS) {
            let secs = elapsed.as_secs_f64();
            info!(
                "Uplink throughput: {:.0} kbit/s ({:.1} frames/s, {:.0} bytes/frame avg)",
                (self.bytes as f64 * 8.0 / 1000.0) / secs,
                self.frames as f64 / secs,
                self.bytes as f64 / self.frames.max(1) as f64,
            );
            self.bytes = 0;
            self.frames = 0;
            self.window_start = Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn telemetry_stub() -> ServerMessage {
        ServerMessage::ConnectionStatus {
            game_connected: true,
            plugin_version: String::new(),
            telemetry_valid: true,
            telemetry_warning: None,
        }
    }

    /// Event-driven messages must never be rate-limited — dropping a lap
    /// snapshot or an engineer callout loses information permanently, unlike
    /// dropping one telemetry frame out of twenty.
    #[test]
    fn non_telemetry_messages_are_never_throttled() {
        let mut throttle = Throttle::new(1);
        assert!(!throttle.should_drop(&telemetry_stub()));
        assert!(!throttle.should_drop(&telemetry_stub()));
        assert!(!throttle.should_drop(&telemetry_stub()));
    }
}
