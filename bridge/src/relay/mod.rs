//! Relay: stream telemetry from the driver's PC to a public server.
//!
//! The same binary plays both sides. On the driver's PC (`--relay-url`) the
//! [`uplink`] task subscribes to the existing broadcast channels and forwards
//! every message out over one WebSocket. On the VPS (`--server`) the [`room`]
//! decodes those frames and pushes them straight back into the same broadcast
//! channels the local dashboard already reads from, so viewer code is unchanged.
//!
//! Phase 1 relays server→viewer traffic only. Viewer commands (Post-Race, Fuel
//! Calculator, Race Engineer) still need the downlink half, which is why
//! [`envelope::UplinkFrame`] is a tagged enum from the start.

pub mod envelope;
pub mod room;
pub mod uplink;

pub use room::RelayRoom;
pub use uplink::task_uplink;
