# Deploying the relay server

The relay server is the **same binary** as the Windows product, run with `--server`.
It serves the dashboard and accepts one streaming agent on `/uplink`. It reads no
shared memory and needs no game. Here it runs as a Docker Compose service behind an
existing Traefik, which terminates TLS — the relay speaks plain HTTP itself.

> **Viewers are not authenticated.** Only the agent needs a key; anyone who has the
> address can watch the telemetry. That is deliberate — teams share the link — but do
> not put the server on an address you would not want found.
>
> **The relay is view-only.** Nothing a viewer does reaches the racing PC: Post-Race,
> the Fuel Calculator and the Race Engineer are refused by the relay *and* by the
> agent, and engineer audio is not uploaded. The Fuel Manager widget covers live fuel
> and VE from the streamed telemetry.

## Requirements

x86_64 Linux with Docker Compose, a Traefik container on a Docker network that other
services join (named `traefik` here — set `TRAEFIK_NETWORK` in `.env` if yours
differs), and a hostname pointing at the server.

## 1. Build (on the development machine)

```bash
make build-server          # dashboard first, then the host-target binary
# → dist/lmu-pitwall-server
```

The dashboard must build first: `bridge/src/assets.rs` embeds `dashboard/dist/` via
`rust-embed`, so a bare `cargo build` ships a stale or missing bundle. The image
packages this binary rather than compiling it; build on glibc 2.35 or older (Ubuntu
22.04 is fine) so it runs on the image's Debian 12.

## 2. Copy to the server

```bash
ssh <host> 'mkdir -p ~/lmu-pitwall'
scp dist/lmu-pitwall-server deploy/Dockerfile deploy/compose.yaml <host>:~/lmu-pitwall/
```

## 3. Configure

On the server, create `~/lmu-pitwall/.env`. The server refuses to start with a key
shorter than 32 characters:

```bash
cd ~/lmu-pitwall
umask 077
cat > .env <<EOF
PITWALL_HOST=pitwall.example.com
PITWALL_AGENT_KEY=$(openssl rand -hex 32)
EOF
```

The key is what agents authenticate with — copy it to each racing PC. Check that the
`entrypoints` and `certresolver` labels in `compose.yaml` match your Traefik.

## 4. Start

```bash
docker compose up -d --build
docker compose logs -f
```

Expect `LMU Bridge v<version>` and `Dashboard available at http://0.0.0.0:9000`. If
the address cannot be bound the container exits, `restart: unless-stopped` retries, and
the reason is in the logs.

Open `https://<your-host>/`. On a server that has never had an agent the dashboard
loads empty; "Agent disconnected" only appears after an agent has connected and left.
The Race Engineer, Fuel Calculator and Post-Race buttons are hidden on a relay.

Agents connect with:

```
lmu-pitwall.exe --relay-url wss://<your-host>/uplink --agent-key <key> --headless
```

To update, copy the new binary over and run `docker compose up -d --build` again.

## Port 9000 stays private

The compose file publishes no ports: Traefik reaches the relay over the shared Docker
network, so 9000 never exists on the host's public interfaces. In the provider's
firewall, open only 22, 80 and 443. Check from a machine *outside* the VPS:
`curl -m5 http://<public-ip>:9000/` must fail.

## Container hardening

Read-only root filesystem (logs go to a tmpfs `/tmp`), all capabilities dropped,
`no-new-privileges`, a non-root user from the distroless image, and memory/PID limits.
The image has no shell; `docker compose exec` will not work, use the logs.

## WebSocket idle timeouts

The agent does not send application-level keepalives, so an uplink carrying no
telemetry — game not running, or sitting in a menu — is an idle connection as far as
Traefik is concerned. Its entrypoint `idleTimeout` defaults to 180s. A drop costs
nothing permanent (the agent reconnects on the 2s/4s/8s/16s/30s ladder), but it is the
likely answer to "the uplink keeps dropping while I'm parked in the garage". Not yet
observed in practice — recorded here so the next person does not start by suspecting
the relay.

## Rotating the key

The key is a **single shared secret**, not per driver, and there is no revocation list.
To rotate it: change `PITWALL_AGENT_KEY` in `.env`, run `docker compose up -d`, and
update every agent. Agents with the old key reconnect forever and are refused with
`401 invalid agent key` each time.

## Notes

- **One agent at a time.** A second agent with the same key takes over and the previous
  one is dropped — that is how driver handover works.
- **Viewers cannot cause work on the racing PC.** The command path from earlier
  versions is switched off by `RELAY_COMMANDS` in `bridge/src/websocket/server.rs`, on
  both ends; `--allow-remote-install` is inert while it is.
- At most 20 viewers at once. A connection has 10 s to send its request headers or
  finish its WebSocket handshake.
- The server ignores `/api/shutdown` and the other process-control routes.
- Mount at the site root. The dashboard builds its WebSocket URL from the origin with
  no path, so a subpath route (`https://host/pitwall/`) will not connect.
