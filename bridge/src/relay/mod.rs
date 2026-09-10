//! Relay: stream telemetry from the driver's PC to a public server.
//!
//! The same binary plays both sides. On the driver's PC (`--relay-url`) the
//! [`uplink`] task subscribes to the existing broadcast channels and forwards
//! every message out over one WebSocket. On the VPS (`--server`) the [`room`]
//! decodes those frames and pushes them straight back into the same broadcast
//! channels the local dashboard already reads from, so viewer code is unchanged.
//!
//! Post-Race and Fuel Calculator commands travel the other way: the room sends
//! an [`envelope::DownlinkFrame`] carrying a server-allocated `req`, and the
//! agent answers with an `UplinkFrame::Response` holding the same `req`. The
//! browser protocol is untouched — request ids exist only on this hop.

pub mod envelope;
pub mod room;
pub mod uplink;

pub use room::RelayRoom;
pub use uplink::task_uplink;
