# rpcproxy

[![CI](https://github.com/nano-trade/rpcproxy/actions/workflows/ci.yml/badge.svg)](https://github.com/nano-trade/rpcproxy/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A fast, hardened public gateway for **Banano** and **Nano** nodes. Put it in
front of your node to safely offer RPC and live confirmations over websocket to
wallets, explorers and bots.

It powers `api.banano.trade` and `ws2.banano.trade`, which serve
[Nault for Banano](https://nault.banano.trade) among others. It is a drop-in
replacement for [NanoRPCProxy](https://github.com/Joohansson/NanoRPCProxy),
written in Go: a single 10 MB static binary with two dependencies.

## Features

- **RPC** at `/proxy`, via `GET /proxy?action=...` or `POST /proxy` with a JSON body.
  Only actions on your allowlist ever reach the node.
- **Websocket confirmations.** Clients subscribe to accounts and get the node's
  confirmation messages for blocks sent from or to them. It speaks both the
  node's protocol (`update` with `accounts_add`/`accounts_del`) and NanoRPCProxy's.
- **Both networks** with correct units: Banano 1 BAN = 10^29 raw, Nano 1 XNO = 10^30 raw.
  `mnano_to_raw`, `mnano_from_raw` and a configurable `price` action are answered by the proxy.
- **Old and new action names**: `pending` and `receivable`, `accounts_pending` and `accounts_receivable`, and so on.
- **Prometheus metrics** on a separate, private port.
- **Docker image** and ready-built binaries for Linux (amd64, arm64, armv7), macOS and FreeBSD.

## Security

| Threat | Protection |
|---|---|
| Wallet, control or other dangerous RPC calls | Action allowlist, unknown actions get `403` |
| One client hogging the node | Per-IP token bucket on RPC and websocket messages, `X-RateLimit-*` headers |
| Spoofed client IPs | Real-IP header only trusted from configured proxies (e.g. nginx on localhost) |
| Huge requests | Body size limit, max items per array, forced max `count` on heavy actions |
| Huge responses | Responses over 32 MB are dropped, concurrent node requests are capped |
| Expensive accounts (e.g. the Nano burn address) | `blocked_accounts` refuses requests that name them |
| Slowloris and idle connections | Timeouts on every phase of HTTP, websocket ping/pong |
| Websocket abuse | Per-IP and global connection caps, per-connection account cap, 64 KB messages, address checksum validation, slow clients get disconnected |
| A compromised proxy | systemd unit runs it as a throwaway user with no disk write access and a syscall filter (`systemd-analyze security`: 1.2) |

## Quick start

Download a release from the [releases page](https://github.com/nano-trade/rpcproxy/releases),
or build it (Go 1.26+):

```sh
go install github.com/nano-trade/rpcproxy@latest
```

Then:

```sh
rpcproxy -check -config config.banano.json   # validate
rpcproxy -config config.banano.json
curl 'http://localhost:9970/proxy?action=block_count'
rpcproxy -version
```

### As a service (Linux, systemd)

One instance per network: `rpcproxy@banano` reads `/etc/rpcproxy/banano.json`.

```sh
install -m 755 rpcproxy /usr/local/bin/
install -m 644 deploy/rpcproxy@.service /etc/systemd/system/
mkdir -p /etc/rpcproxy && cp config.banano.json /etc/rpcproxy/banano.json
systemctl daemon-reload && systemctl enable --now rpcproxy@banano
```

For Nano, use `config.nano.json` as `/etc/rpcproxy/nano.json` and enable
`rpcproxy@nano`. Both can run side by side.

### With Docker

```sh
docker run -d --name rpcproxy --network host --restart unless-stopped \
  -v /etc/rpcproxy/banano.json:/etc/rpcproxy/config.json:ro \
  ghcr.io/nano-trade/rpcproxy:latest
```

Host networking lets the default `localhost` addresses reach the node and nginx
just like a normal install. The image has no shell and runs as a non-root user.

### Behind nginx and Cloudflare

See [`deploy/nginx-example.conf`](deploy/nginx-example.conf). nginx must pass
`X-Real-IP`, otherwise every visitor shares one rate limit.

[`deploy/update-cloudflare-ips`](deploy/update-cloudflare-ips) builds the
nginx config from Cloudflare's published IP ranges:

- **Default (per site):** `conf.d/rpcproxy-cloudflare-geo.conf` defines
  `$via_cloudflare`, and `snippets/rpcproxy-cloudflare-realip.conf` restores the
  visitor IP. Only the server blocks that `include` the snippet are affected.
- **`--global`:** one `conf.d/cloudflare.conf` that restores visitor IPs for
  every site on the server.

It never overwrites a file it didn't generate, keeps the old files if the
download fails, and rolls back if `nginx -t` fails. Install it with the timer
for weekly refreshes:

```sh
install -m 755 deploy/update-cloudflare-ips /usr/local/sbin/
install -m 644 deploy/update-cloudflare-ips.{service,timer} /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now update-cloudflare-ips.timer
```

## Using it

### RPC

```sh
curl 'https://api.example.com/proxy?action=account_balance&account=ban_1...'
curl -d '{"action":"accounts_balances","accounts":["ban_1...","ban_3..."]}' https://api.example.com/proxy
```

Arrays work in GET as `accounts[]=a&accounts[]=b`. Responses are the node's
JSON unchanged. Errors are JSON `{"error": "..."}` with:

| Status | Meaning |
|---|---|
| `400` | Malformed request, too many items |
| `403` | Action not allowed, blocked IP or account |
| `413` | Body too large |
| `429` | Rate limited, see `Retry-After` |
| `502` / `503` | Node unavailable or busy |

Every RPC response carries `X-RateLimit-Limit` (burst size),
`X-RateLimit-Remaining` and `X-RateLimit-Reset` (seconds until the full budget
is back).

### Websocket

```json
{"action":"subscribe","topic":"confirmation","id":"my-1","options":{"accounts":["ban_1..."]}}
```
→ `{"ack":"subscribe","time":"1792669909","id":"my-1"}`, then the node's
confirmation messages for those accounts.

| Message | Effect |
|---|---|
| `subscribe` with `options.accounts` | Adds accounts |
| `update` with `options.accounts_add` / `accounts_del` (or `options.accounts`) | Adds and removes accounts |
| `unsubscribe` | Removes all accounts, or only those in `options.accounts` |
| `ping` | Replies `{"ack":"pong","time":"<ms>"}` |

Every reply includes the `id` you sent, if any.

### Metrics

Set `metrics_listen` (e.g. `localhost:9978`) and scrape
`http://localhost:9978/metrics` with Prometheus. It covers requests by action and
result, node latency per action, cache hits, websocket clients, subscriptions,
forwarded messages and whether the node websocket is up. Keep this port private;
it is never served on the public RPC port.

## Configuration

Unknown keys are rejected, so typos are caught at startup.

| Key | Meaning |
|---|---|
| `network` | `banano` or `nano`: address prefixes and unit size |
| `listen` | RPC address. `localhost:port` listens on both 127.0.0.1 and ::1 |
| `request_path` | URL path for RPC (default `/proxy`) |
| `node_rpc_url` | Node RPC, e.g. `http://[::1]:7072` (Banano) or `:7076` (Nano) |
| `node_headers` | Extra headers for the node, e.g. `{"Authorization": "..."}` for a remote node |
| `trusted_proxies` | IPs or CIDRs allowed to set `real_ip_header` |
| `real_ip_header` | Header carrying the visitor IP (default `X-Real-IP`) |
| `allowed_actions` | RPC actions passed to the node |
| `cached_actions` | Action → seconds to cache. Identical requests share an entry and concurrent ones are merged. Keep `block_count` at 1–2 s: some clients rank nodes by block count |
| `limited_actions` | Action → maximum `count` (missing or larger values are capped) |
| `blocked_accounts` | Requests whose `account` or `accounts` name one of these get `403` |
| `max_array_items` | Max items in any array parameter |
| `max_body_bytes` | Max POST body size |
| `rpc_timeout_seconds` | Node request timeout |
| `max_concurrent_node_requests` | Requests in flight to the node at once |
| `rate_limit` | `per_second` sustained and `burst` per client IP |
| `cors_origins` | Allowed browser origins, empty means any |
| `price_url` | JSON URL returned by the `price` action, empty disables it |
| `price_cache_seconds` | How long to cache the price |
| `ip_blocklist` | IPs or CIDRs to refuse |
| `log_requests` | Log every request (otherwise a summary every 10 minutes) |
| `metrics_listen` | Prometheus address, empty disables metrics |
| `websocket.listen` | Websocket address, empty disables it |
| `websocket.node_url` | Node websocket, e.g. `ws://[::1]:7079` (Banano) or `:7078` (Nano) |
| `websocket.max_connections` | Total websocket connections |
| `websocket.max_connections_per_ip` | Connections per visitor IP |
| `websocket.max_accounts_per_message` | Accounts in one subscribe/update |
| `websocket.max_accounts_per_connection` | Accounts one connection may watch |

## Coming from NanoRPCProxy

Most clients need no change. The example configs use different ports
(Banano 9970/9972, Nano 9980/9982) than NanoRPCProxy's 9950/9952, so both can
run side by side while you switch nginx over. Differences:

- Websocket subscriptions are tracked per connection. NanoRPCProxy tracks them
  per IP, which mixes up users behind the same IP, and behind nginx or
  Cloudflare that is everyone.
- The cache key includes the request parameters.
- Unit conversions are correct for Banano.
- Disallowed actions return `403` instead of `500`.
- Left out on purpose: the token purchase system, basic-auth users, dPoW/BoomPoW
  work delegation and the daily stats file. `work_generate` can still be
  allowlisted and is then handled by the node. `disable_watch_work` is accepted
  but ignored, since nodes ignore `watch_work` since V22.
- `verified_accounts` is not supported. NanoRPCProxy fetched it from the
  mynano.ninja API, which no longer responds; requests for it get `403`.

## Development

```sh
go test -race ./...
go vet ./...
```

Releases: push a tag like `v1.2.0`. GitHub Actions builds the binaries, attaches
them with checksums to a GitHub release, and publishes the multi-arch image to
`ghcr.io/nano-trade/rpcproxy`.

## License

MIT, see [LICENSE](LICENSE).
