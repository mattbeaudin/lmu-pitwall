import { useSettingsStore } from '../stores/settingsStore'

/**
 * Where the bridge lives, and how to reach it.
 *
 * With no override the page is being served *by* the bridge, so its own origin
 * is the answer — `location.host` already carries the port when there is one,
 * and omits it on 80/443. That is what makes a reverse-proxied deployment work:
 * hardcoding a port breaks TLS and standard-port serving alike.
 *
 * The scheme is derived rather than fixed so a page served over HTTPS opens a
 * `wss://` socket; browsers block a plaintext socket from a secure page.
 */
function resolveTarget(): { host: string; secure: boolean } {
  const { wsHost, wsPort } = useSettingsStore.getState()
  const secure = window.location.protocol === 'https:'
  const overrideHost = wsHost.trim()

  if (!overrideHost && wsPort <= 0) {
    return { host: window.location.host, secure }
  }

  const host = overrideHost || window.location.hostname
  const port = wsPort > 0 ? String(wsPort) : (window.location.port || '9000')
  return { host: `${host}:${port}`, secure }
}

export function bridgeWsUrl(): string {
  const { host, secure } = resolveTarget()
  return `${secure ? 'wss' : 'ws'}://${host}`
}

export function bridgeHttpBase(): string {
  const { host, secure } = resolveTarget()
  return `${secure ? 'https' : 'http'}://${host}`
}
