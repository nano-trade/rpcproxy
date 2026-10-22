# rpcproxy

A small, fast and hardened public proxy for **Banano** and **Nano** node RPC and
websocket confirmations. It is a drop-in replacement for
[NanoRPCProxy](https://github.com/Joohansson/NanoRPCProxy) (Node.js) for the
public-facing parts that wallets such as Nault use, written in Go as a single
static binary with two dependencies.

## What it does

- **RPC** at `/proxy`: `GET /proxy?action=...` or `POST /proxy` with a JSON body.
  Only actions in `allowed_actions` reach the node; everything else gets `403`.
- **Websocket** for confirmations: clients subscribe to up to N accounts and
  receive the node's confirmation messages for blocks sent from or to them.
- **Extra actions** handled by the proxy itself: `price`, `mnano_to_raw`,
  `mnano_from_raw` (using the right unit size for each network: Banano 10^29
  raw, Nano 10^30 raw).

## Security

- Per-client rate limiting (token bucket) on RPC and websocket messages. The real
  client IP is only taken from `X-Real-IP` when the request comes from a trusted
  proxy (nginx on localhost), so it can't be spoofed.
- Request limits: body size, number of items in any array (`accounts`, `hashes`, …),
  and a forced maximum `count` for heavy actions (`account_history`, `pending`, …).
- Never forwards node responses larger than 32 MB; caps concurrent node requests.
- HTTP timeouts on every phase (slowloris protection), security headers.
- Websocket: per-IP and global connection caps, per-connection account cap,
  64 KB message limit, ping/pong keepalive, slow clients get disconnected
  instead of using memory, addresses validated including checksum.
- The systemd unit runs it as a throwaway user with no write access to the
  disk, no capabilities and a syscall filter (`systemd-analyze security` 1.2/10).

## Differences from NanoRPCProxy

- Subscriptions are per websocket connection. The old proxy tracked them per IP,
  which broke when many users came through the same IP (e.g. behind nginx or
  Cloudflare, where everyone shared one entry).
- The cache key includes the request parameters (the old one cached by action name only).
- Unit conversions are correct for Banano.
- Disallowed actions return `403` (was `500`); bad requests return `400`, rate
  limits `429` with JSON `{"error": "..."}`.
- Not included: the Nano token purchase system, basic-auth users, dPoW/BoomPoW
  work delegation and the daily request-stat file. `work_generate` can still be
  allowed and is then sent to the node.

## Build

Requires Go 1.26+.

```sh
go test -race ./...
go build -trimpath -ldflags "-s -w -X main.version=1.0.0" -o rpcproxy .
```

## Run

```sh
./rpcproxy -check -config config.banano.json   # validate config
./rpcproxy -config config.banano.json
```

On a server (one instance per network):

```sh
install -m 755 rpcproxy /usr/local/bin/
install -m 644 deploy/rpcproxy@.service /etc/systemd/system/
mkdir -p /etc/rpcproxy && cp config.banano.json /etc/rpcproxy/banano.json
systemctl daemon-reload && systemctl enable --now rpcproxy@banano
```

For Nano, copy `config.nano.json` to `/etc/rpcproxy/nano.json` and enable
`rpcproxy@nano`. Both can run side by side on different ports.

Behind nginx + Cloudflare, see `deploy/nginx-example.conf` and
`deploy/update-cloudflare-ips` (restores visitor IPs and blocks requests that
bypass Cloudflare). nginx must send `X-Real-IP`.

## Config

| Key | Meaning |
|---|---|
| `network` | `banano` or `nano`: address prefixes and unit size |
| `listen` | RPC listen address, keep it on localhost behind nginx (`localhost:port` binds both 127.0.0.1 and ::1) |
| `request_path` | URL path for RPC (default `/proxy`) |
| `node_rpc_url` | node RPC, e.g. `http://[::1]:7072` (Banano) / `:7076` (Nano) |
| `trusted_proxies` | IPs/CIDRs allowed to set `real_ip_header` |
| `allowed_actions` | RPC actions that are passed to the node |
| `cached_actions` | action → seconds to cache (same parameters share a cache entry) |
| `limited_actions` | action → maximum `count` |
| `max_array_items` | max items in any array parameter |
| `max_body_bytes` | max POST body size |
| `rate_limit` | `per_second` sustained and `burst` per client IP |
| `max_concurrent_node_requests` | requests in flight to the node at once |
| `cors_origins` | allowed browser origins, empty = any |
| `disable_watch_work` | force `watch_work=false` on `process` |
| `price_url` | JSON URL returned by the `price` action (empty disables it) |
| `ip_blocklist` | IPs/CIDRs to refuse |
| `log_requests` | log every request (otherwise a summary every 10 minutes) |
| `websocket.listen` | websocket listen address, empty disables it |
| `websocket.node_url` | node websocket, e.g. `ws://[::1]:7079` (Banano) / `:7078` (Nano) |
| `websocket.max_*` | connection and subscription caps |

Unknown keys are rejected, so typos are caught at startup.

## Websocket protocol

```json
{"action":"subscribe","topic":"confirmation","options":{"accounts":["ban_1..."]}}
```
→ `{"ack":"subscribe"}`, then the node's confirmation messages for those accounts.

```json
{"action":"unsubscribe","topic":"confirmation"}
```
removes all accounts (or only those given in `options.accounts`).
`{"action":"ping"}` → `{"ack":"pong","time":...}`.
