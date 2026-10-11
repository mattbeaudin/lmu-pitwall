# Deploying the relay server

The relay server is the **same binary** as the Windows product, run with `--server`.
It serves the dashboard and accepts one streaming agent on `/uplink`. It reads no
shared memory and needs no game.

> **Viewers are not authenticated.** Only the agent needs a key; anyone who has the
> address can watch the telemetry. That is deliberate — teams share the link — but do
> not put the server on an address you would not want found.
>
> **The relay is view-only.** Nothing a viewer does reaches the racing PC: Post-Race,
> the Fuel Calculator and the Race Engineer are refused by the relay *and* by the
> agent, and engineer audio is not uploaded. The Fuel Manager widget covers live fuel
> and VE from the streamed telemetry.

## Requirements

x86_64 Linux with **glibc 2.35 or newer** (Ubuntu 22.04+, Debian 12+). The binary is
dynamically linked against the glibc of the machine that built it; a build from Ubuntu
22.04 will not start on Debian 11 or Ubuntu 20.04. Nothing else is needed — TLS,
SQLite and the dashboard are all inside the executable.

## 1. Build (on the development machine)

```bash
make build-server          # dashboard first, then the host-target binary
# → dist/lmu-pitwall-server
```

The dashboard must build first: `bridge/src/assets.rs` embeds `dashboard/dist/` via
`rust-embed`, so a bare `cargo build` ships a stale or missing bundle.

## 2. Install

```bash
ssh <host> 'sudo mkdir -p /opt/lmu-pitwall'
scp dist/lmu-pitwall-server <host>:/tmp/
ssh <host> 'sudo install -m755 /tmp/lmu-pitwall-server /opt/lmu-pitwall/'

scp deploy/lmu-pitwall.service <host>:/tmp/
ssh <host> 'sudo install -m644 /tmp/lmu-pitwall.service /etc/systemd/system/'
```

Set the shared key — it is what agents authenticate with. The server refuses to start
with a key shorter than 32 characters:

```bash
sudo install -d -m700 /etc/lmu-pitwall
echo "PITWALL_AGENT_KEY=$(openssl rand -hex 32)" | sudo tee /etc/lmu-pitwall/agent.env >/dev/null
sudo chmod 600 /etc/lmu-pitwall/agent.env
sudo systemctl daemon-reload
sudo systemctl enable --now lmu-pitwall
```

The key lives in a root-only file, not in the unit. Unit files and `systemctl edit`
overrides are world-readable and `systemctl show` prints `Environment=` to any user;
flags are no better, they show up in `ps`.

## 3. Keep port 9000 private

The relay speaks plain HTTP, so it belongs behind a reverse proxy that terminates TLS,
and 9000 should never be reachable from the internet. Two layers:

- **Bind.** The unit runs `--bind 127.0.0.1`, which only a proxy on the same host can
  reach. Behind Traefik in Docker, bind the docker0 address instead (see "Traefik").
- **Firewall.** In the provider's firewall or security group, open only 22, 80 and 443.
  On the host:

  ```bash
  sudo ufw default deny incoming
  sudo ufw allow 22/tcp && sudo ufw allow 80/tcp && sudo ufw allow 443/tcp
  ```

  Behind Traefik in Docker, also allow the bridge range to reach 9000 — host ufw rules
  apply to a host process like the relay, unlike Docker-published ports:

  ```bash
  sudo ufw allow from 172.16.0.0/12 to any port 9000 proto tcp
  ```

Check from a machine *outside* the VPS: `curl -m5 http://<public-ip>:9000/` must fail.

## 4. Verify

```bash
journalctl -u lmu-pitwall -n 20
```

Expect `LMU Bridge v<version>` and `Dashboard available at http://127.0.0.1:9000`. If
the address cannot be bound — port in use, or the docker0 address not up yet — the
server exits and systemd retries every 5 s; the reason is in the journal.

Then score the sandbox; the unit should come out in the "OK" band or better:

```bash
systemd-analyze security lmu-pitwall
```

Open `https://<your-host>/`. On a server that has never had an agent the dashboard
loads empty; "Agent disconnected" only appears after an agent has connected and left.
The Race Engineer, Fuel Calculator and Post-Race buttons are hidden on a relay.

If the sandbox stops the service from starting, `journalctl -u lmu-pitwall` names the
blocked call or path; relax that one directive with `systemctl edit` rather than
dropping the rest.

## TLS via Caddy

Point a hostname at the server, then:

```bash
sudo apt install caddy
sudo cp deploy/Caddyfile /etc/caddy/Caddyfile   # edit the hostname first
sudo systemctl reload caddy
```

Caddy handles both the certificate and the WebSocket upgrade with no extra
configuration. Agents then use `wss://your-host/uplink`, and the dashboard derives
`wss://` from the page URL on its own — leave the host and port fields in dashboard
settings empty.

### Traefik (already running, in Docker)

Traefik's **docker provider cannot express this route** — it derives a service address
from the container carrying the labels, and the relay has no container. Add a file
provider to Traefik's static command; it is the only change needed, and every existing
docker-label route keeps working:

```yaml
      - "--providers.file.directory=/etc/traefik/dynamic"
      - "--providers.file.watch=true"
```

plus, on the same service, a read-only mount of that directory and a route to the host:

```yaml
    volumes:
      - ./traefik-dynamic:/etc/traefik/dynamic:ro
    extra_hosts:
      - "host.docker.internal:host-gateway"
```

Then copy `deploy/traefik-lmu-pitwall.yml` into that directory and set the hostname.

`host.docker.internal` resolves to the docker0 address (normally `172.17.0.1`; check
with `ip -4 addr show docker0`), so the relay must listen there rather than on loopback.
In the unit, via `sudo systemctl edit --full lmu-pitwall`, change `--bind 127.0.0.1` to
`--bind 172.17.0.1` and uncomment the `After=`/`Requires=docker.service` lines —
docker0 only exists once Docker is up.
Adding the file provider is a static-config change, so the Traefik container has to be
recreated once (`docker compose up -d traefik`) — a few seconds of 502 for everything
else it fronts. The dynamic file itself is watched and needs no restart afterwards.

### nginx equivalent

```nginx
location / {
    proxy_pass http://127.0.0.1:9000;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_read_timeout 1h;     # without this, idle sockets get cut
}
```

Mount at the site root. The dashboard builds its WebSocket URL from the origin with no
path, so a subpath mount (`https://host/pitwall/`) will not connect.

## WebSocket idle timeouts

Whatever proxy is in front, check its idle timeout. The agent does not send
application-level keepalives, so an uplink carrying no telemetry — game not running,
or sitting in a menu — is an idle connection as far as the proxy is concerned.
Traefik's entrypoint `idleTimeout` defaults to 180s and nginx's `proxy_read_timeout`
to 60s. A drop costs nothing permanent (the agent reconnects on the 2s/4s/8s/16s/30s
ladder), but it is the likely answer to "the uplink keeps dropping while I'm parked in
the garage". Not yet observed in practice — recorded here so the next person does not
start by suspecting the relay.

## Rotating the key

The key is a **single shared secret**, not per driver, and there is no revocation list.
To rotate it: change `PITWALL_AGENT_KEY` in the unit override, `systemctl restart
lmu-pitwall`, and update every agent. Agents with the old key reconnect forever and
are refused with `401 invalid agent key` each time.

## Notes

- **One agent at a time.** A second agent with the same key takes over and the previous
  one is dropped — that is how driver handover works.
- **Viewers cannot cause work on the racing PC.** The command path from earlier
  versions is switched off by `RELAY_COMMANDS` in `bridge/src/websocket/server.rs`, on
  both ends; `--allow-remote-install` is inert while it is.
- At most 20 viewers at once. A connection has 10 s to send its request headers or
  finish its WebSocket handshake.
- The server ignores `/api/shutdown` and the other process-control routes.
- `/opt/lmu-pitwall` stays read-only under the unit's `ProtectSystem=strict`. The
  server has nothing to write there — the results database is only ever opened by an
  agent — and the unit points `$APPDATA` at the state directory so the log file has
  somewhere to go.
