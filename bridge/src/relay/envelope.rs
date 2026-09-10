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

use crate::protocol::messages::{ClientCommand, ServerMessage};

/// Agent → server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UplinkFrame {
    /// First frame after the handshake. Identifies the streaming agent.
    Hello { agent_version: String },
    /// Fan out to every viewer — everything `task_broadcaster` produces.
    Message(ServerMessage),
    /// Answer to exactly one viewer's request, identified by its `req`.
    Response { req: u64, msg: ServerMessage },
}

/// Server → agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DownlinkFrame {
    /// Run this command on the driver's PC and answer with the same `req`.
    ///
    /// `req` is allocated by the server; the agent only echoes it back, so a
    /// viewer's identity never goes on the wire.
    Command { req: u64, cmd: ClientCommand },
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

    /// The `req` is the whole correctness argument for N viewers sharing one
    /// agent, so it has to survive the round-trip alongside the message.
    #[test]
    fn response_frame_round_trips_through_msgpack() {
        let original = UplinkFrame::Response {
            req: 42,
            msg: ServerMessage::PostRaceError {
                message: "no results".to_string(),
            },
        };

        let bytes = rmp_serde::to_vec_named(&original).unwrap();
        let decoded: UplinkFrame = rmp_serde::from_slice(&bytes).unwrap();

        match decoded {
            UplinkFrame::Response {
                req,
                msg: ServerMessage::PostRaceError { message },
            } => {
                assert_eq!(req, 42);
                assert_eq!(message, "no results");
            }
            other => panic!("wrong frame after round-trip: {:?}", other),
        }
    }

    #[test]
    fn command_frame_round_trips_through_msgpack() {
        let original = DownlinkFrame::Command {
            req: 7,
            cmd: ClientCommand::PostRaceDriverLaps { driver_id: 99 },
        };

        let bytes = rmp_serde::to_vec_named(&original).unwrap();
        let decoded: DownlinkFrame = rmp_serde::from_slice(&bytes).unwrap();

        match decoded {
            DownlinkFrame::Command {
                req,
                cmd: ClientCommand::PostRaceDriverLaps { driver_id },
            } => {
                assert_eq!(req, 7);
                assert_eq!(driver_id, 99);
            }
            other => panic!("wrong frame after round-trip: {:?}", other),
        }
    }
}
