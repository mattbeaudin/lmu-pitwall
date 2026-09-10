//! Wire frames for the relay hop.
//!
//! These wrap [`ServerMessage`] only between agent and server. The browser
//! protocol is untouched: the server unwraps every frame before it reaches a
//! viewer socket, so `dashboard/` needs no knowledge of any of this.
//!
//! Externally tagged (serde's default) rather than internally tagged —
//! `ServerMessage` is itself internally tagged on `type`, and nesting the two
//! in one map is needlessly fragile across MessagePack round-trips.

use serde::{Deserialize, Serialize};

use crate::protocol::messages::ServerMessage;

/// Agent → server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UplinkFrame {
    /// First frame after the handshake. Identifies the streaming agent.
    Hello { agent_version: String },
    /// Fan out to every viewer — everything `task_broadcaster` produces.
    Message(ServerMessage),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The whole design rests on this: `ServerMessage` survives a MessagePack
    /// round-trip inside an envelope, so relaying telemetry needs no new
    /// message types on either side.
    #[test]
    fn message_frame_round_trips_through_msgpack() {
        let original = ServerMessage::ConnectionStatus {
            game_connected: true,
            plugin_version: "1.2.3".to_string(),
            telemetry_valid: true,
            telemetry_warning: None,
        };

        let bytes = rmp_serde::to_vec_named(&UplinkFrame::Message(original)).unwrap();
        let decoded: UplinkFrame = rmp_serde::from_slice(&bytes).unwrap();

        match decoded {
            UplinkFrame::Message(ServerMessage::ConnectionStatus {
                game_connected,
                plugin_version,
                telemetry_valid,
                telemetry_warning,
            }) => {
                assert!(game_connected);
                assert_eq!(plugin_version, "1.2.3");
                assert!(telemetry_valid);
                assert_eq!(telemetry_warning, None);
            }
            other => panic!("wrong frame after round-trip: {:?}", other),
        }
    }

    #[test]
    fn hello_frame_round_trips_through_msgpack() {
        let bytes = rmp_serde::to_vec_named(&UplinkFrame::Hello {
            agent_version: "9.9.9".to_string(),
        })
        .unwrap();
        let decoded: UplinkFrame = rmp_serde::from_slice(&bytes).unwrap();
        match decoded {
            UplinkFrame::Hello { agent_version } => assert_eq!(agent_version, "9.9.9"),
            other => panic!("wrong frame after round-trip: {:?}", other),
        }
    }
}
