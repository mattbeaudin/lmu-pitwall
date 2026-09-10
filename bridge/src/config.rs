use clap::Parser;

/// How this process runs. Derived once from [`Config`] in `main`.
///
/// The same binary is all three: a driver's PC serving its own dashboard
/// (`Local`), a driver's PC streaming to a VPS (`Agent`), and the VPS itself
/// serving that stream publicly (`Server`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Today's behaviour — poll shared memory, serve the dashboard locally.
    Local,
    /// Poll shared memory and stream it to a relay. Dashboard optional.
    Agent,
    /// Serve the dashboard from an agent's uplink. No shared-memory polling.
    Server,
}

#[derive(Parser, Debug)]
#[command(name = "lmu-bridge", about = "Le Mans Ultimate Data Bridge")]
pub struct Config {
    /// WebSocket server port (overrides config.json when provided)
    #[arg(long)]
    pub ws_port: Option<u16>,

    /// Telemetry broadcast rate in FPS
    #[arg(long, default_value_t = 20)]
    pub telemetry_fps: u32,

    /// Scoring broadcast rate in FPS
    #[arg(long, default_value_t = 20)]
    pub scoring_fps: u32,

    /// Enable LMU REST API polling (Phase 2)
    #[arg(long, default_value_t = false)]
    pub enable_rest_api: bool,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    pub log_level: String,

    /// Do not open the browser automatically on startup
    #[arg(long, default_value_t = false)]
    pub no_browser: bool,

    /// Stream telemetry to this relay instead of only serving it locally,
    /// e.g. `wss://pitwall.example.com/uplink`. Implies agent mode.
    #[arg(long)]
    pub relay_url: Option<String>,

    /// Shared secret for the uplink. Falls back to `$PITWALL_AGENT_KEY`.
    #[arg(long)]
    pub agent_key: Option<String>,

    /// Run as the relay: serve the dashboard and accept one agent uplink.
    #[arg(long, default_value_t = false)]
    pub server: bool,

    /// Agent mode only — do not bind the local dashboard port.
    #[arg(long, default_value_t = false)]
    pub headless: bool,

    /// Agent mode only — let relay viewers install Piper and voices on this
    /// PC. Off by default: viewers are unauthenticated, and an install writes
    /// a downloaded binary to the driver's machine.
    #[arg(long, default_value_t = false)]
    pub allow_remote_install: bool,

    /// Uplink send rate for telemetry and scoring frames. Deliberately lower
    /// than `--telemetry-fps`: the uplink runs over a home upstream link,
    /// where a full grid's `ScoringUpdate` is the dominant cost.
    #[arg(long, default_value_t = 10)]
    pub uplink_fps: u32,
}

impl Config {
    pub fn mode(&self) -> Mode {
        if self.server {
            Mode::Server
        } else if self.relay_url.is_some() {
            Mode::Agent
        } else {
            Mode::Local
        }
    }

    /// Agent key from the flag, else `$PITWALL_AGENT_KEY`.
    ///
    /// The env var is preferred in deployment so the secret stays out of `ps`.
    pub fn resolved_agent_key(&self) -> Option<String> {
        self.agent_key
            .clone()
            .or_else(|| std::env::var("PITWALL_AGENT_KEY").ok())
            .filter(|k| !k.is_empty())
    }
}
