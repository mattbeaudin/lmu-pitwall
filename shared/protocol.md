# LMU Dashboard — WebSocket Protocol

## Transport

- **Protocol:** WebSocket (`ws://HOST:9000`)
- **Format:** MessagePack (binary, compact)
- **Debug mode:** JSON via query param `?format=json`

## Relay transport (agent → server)

Optional second hop, used when the dashboard is served from a VPS instead of the
driver's PC. The browser protocol below is **unchanged** — the relay unwraps
every frame before it reaches a viewer socket.

- **Agent** (`--relay-url wss://host/uplink --agent-key KEY`) reads shared memory
  and streams what it would have broadcast locally.
- **Server** (`--server`) serves the dashboard and accepts one agent on
  `/uplink`. A second agent takes over; the previous one is dropped.

**Auth.** The agent sends `Authorization: Bearer <key>`; a mismatch is answered
`401` at the handshake. Viewers are unauthenticated — **anyone with the URL sees
the telemetry.**

**Framing.** Binary MessagePack, externally tagged (`ServerMessage` is itself
internally tagged on `type`, and nesting the two in one map round-trips poorly):

| Frame | Shape | Meaning |
|-------|-------|---------|
| `Hello` | `{"Hello":{"agent_version":"1.2.3"}}` | Agent → server. First frame after the handshake |
| `Message` | `{"Message":{<ServerMessage>}}` | Agent → server. Fan out to every viewer |
| `Command` | `{"Command":{"req":7,"cmd":{<ClientCommand>}}}` | Server → agent. Run this on the driver's PC |
| `Response` | `{"Response":{"req":7,"msg":{<ServerMessage>}}}` | Agent → server. Answer for exactly one viewer |

**Rate.** `--uplink-fps` (default 10) limits `TelemetryUpdate` and
`ScoringUpdate` only. Event-driven messages are never dropped.

**Relay rules.** `EngineerAudio` goes to the audio channel so `display_only`
viewers never receive WAV payloads. `VersionInfo` is *not* relayed — the server
reports its own version. `AllDriversUpdate` and `ConnectionStatus` also update
the connect-time replay state so late-joining viewers see the session.

**Commands.** Post-Race and Fuel Calculator commands make a round trip to the
agent; every other command class, including the Race Engineer's, stays local.
`ClientCommand` has no request id, so the server allocates `req` and matches the
`Response` back to the viewer that asked — the browser protocol is unchanged and
a viewer's identity never goes on the wire.

`PostRaceInit` waits 30 s (a cold import parses every result XML), everything
else 10 s. On timeout, or with no agent connected, the viewer gets
`PostRaceError` or `FuelCalcError` — whichever its panel renders.

**Limits.** Viewers are unauthenticated, so each socket is capped at 5 commands
per second and the agent runs at most 4 at once; over either limit the answer is
the same error variant, not silence.

## Message Types

All messages are tagged with a `type` field.

### `TelemetryUpdate` (~30Hz)

High-frequency per-frame telemetry data.

| Field | Type | Description |
|-------|------|-------------|
| `speed_ms` | f64 | Speed in m/s |
| `rpm` | f64 | Engine RPM |
| `max_rpm` | f64 | Rev limiter RPM |
| `gear` | i32 | -1=Reverse, 0=Neutral, 1-8 |
| `throttle` | f64 | 0.0–1.0 |
| `brake` | f64 | 0.0–1.0 |
| `clutch` | f64 | 0.0–1.0 |
| `steering` | f64 | -1.0 to +1.0 |
| `fuel` | f64 | Liters remaining |
| `fuel_capacity` | f64 | Tank capacity in liters |
| `water_temp` | f64 | °C |
| `oil_temp` | f64 | °C |
| `tires` | TireData[4] | FL, FR, RL, RR |
| `delta_best` | f64 | Delta to best lap (seconds) |

### TireData

| Field | Type | Description |
|-------|------|-------------|
| `temp_inner` | f64 | Inner temperature °C |
| `temp_mid` | f64 | Middle temperature °C |
| `temp_outer` | f64 | Outer temperature °C |
| `pressure` | f64 | kPa |
| `wear` | f64 | 0.0–1.0 (1.0 = new) |
| `brake_temp` | f64 | °C |

### `ScoringUpdate` (~20Hz)

Session and standings data.

Most of it comes from LMU's scoring block, which the game rewrites at 5 Hz — so
those fields repeat between changes no matter how often this message is sent.

`pos_x` / `pos_y` / `pos_z` are the exception. They are read from the telemetry
rows, which tick at 100 Hz, so a position is fresh on every send. A car whose
telemetry row is missing, zeroed, or more than 150 m from where scoring puts it
falls back to the scoring position for that field; consumers cannot tell which
source a given car used, and do not need to — the two are the same quantity.

### `SessionInfo` (~1Hz)

Track info, weather, session type/duration.

### `ConnectionStatus` (event-based)

Sent when LMU connects or disconnects, and whenever `telemetry_valid` changes.

| Field | Type | Notes |
|---|---|---|
| `game_connected` | bool | |
| `plugin_version` | string | LMU's build number as `1.4.0.0`; `unknown` if unreadable. Named for the plugin era, when it carried a third-party DLL's version |
| `telemetry_valid` | bool | `false` = shared-memory layout drift detected |
| `telemetry_warning` | string \| null | reason, only set when `telemetry_valid` is `false` |

`telemetry_valid` goes `false` when the per-wheel block fails a plausibility
check, which means a game update moved the wheel block inside
`rF2VehicleTelemetry`. Tire and brake readings are garbage in that state.

Consumers must treat the tire block in `Telemetry` as unusable while
`telemetry_valid` is `false` — the bridge keeps sending it rather than dropping
fields, so the flag is the only signal. In this repo: `TireMonitor` greys the
cells out, and the race engineer's tire rules stay silent.
