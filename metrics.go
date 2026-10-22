package main

import (
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

// Request outcomes, used as the "result" label.
const (
	resultOK          = "ok"
	resultRateLimited = "rate_limited"
	resultRejected    = "rejected"
	resultNodeError   = "node_error"
)

// metrics collects counters for the Prometheus text format. It is written by
// hand to avoid a dependency; the label sets are small and bounded (actions
// come from the allowlist, anything else is reported as "_other").
type metrics struct {
	mu          sync.Mutex
	requests    map[[2]string]int64 // {action, result} -> count
	nodeSeconds map[string]float64  // action -> total node time
	nodeCalls   map[string]int64
	cacheHits   int64
	wsConnects  int64
	wsRejected  int64
	started     time.Time
}

func newMetrics() *metrics {
	return &metrics{
		requests:    map[[2]string]int64{},
		nodeSeconds: map[string]float64{},
		nodeCalls:   map[string]int64{},
		started:     time.Now(),
	}
}

func (m *metrics) request(action, result string) {
	m.mu.Lock()
	m.requests[[2]string{action, result}]++
	m.mu.Unlock()
}

func (m *metrics) nodeCall(action string, d time.Duration) {
	m.mu.Lock()
	m.nodeSeconds[action] += d.Seconds()
	m.nodeCalls[action]++
	m.mu.Unlock()
}

func (m *metrics) add(field *int64) {
	m.mu.Lock()
	*field++
	m.mu.Unlock()
}

// handler serves /metrics. hub may be nil when the websocket is disabled.
func (m *metrics) handler(cfg *Config, hub *Hub) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			http.NotFound(w, r)
			return
		}
		var b strings.Builder
		metric := func(name, typ, help string) {
			fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
		}

		m.mu.Lock()
		metric("rpcproxy_requests_total", "counter", "RPC requests by action and result (ok, rate_limited, rejected, node_error).")
		reqKeys := make([][2]string, 0, len(m.requests))
		for k := range m.requests {
			reqKeys = append(reqKeys, k)
		}
		sort.Slice(reqKeys, func(i, j int) bool {
			return reqKeys[i][0]+reqKeys[i][1] < reqKeys[j][0]+reqKeys[j][1]
		})
		for _, k := range reqKeys {
			fmt.Fprintf(&b, "rpcproxy_requests_total{action=%q,result=%q} %d\n", k[0], k[1], m.requests[k])
		}
		actions := make([]string, 0, len(m.nodeCalls))
		for a := range m.nodeCalls {
			actions = append(actions, a)
		}
		sort.Strings(actions)
		metric("rpcproxy_node_request_seconds", "summary", "Time spent waiting for the node, by action.")
		for _, a := range actions {
			fmt.Fprintf(&b, "rpcproxy_node_request_seconds_sum{action=%q} %g\n", a, m.nodeSeconds[a])
			fmt.Fprintf(&b, "rpcproxy_node_request_seconds_count{action=%q} %d\n", a, m.nodeCalls[a])
		}
		metric("rpcproxy_cache_hits_total", "counter", "Requests answered from the cache.")
		fmt.Fprintf(&b, "rpcproxy_cache_hits_total %d\n", m.cacheHits)
		wsConnects, wsRejected := m.wsConnects, m.wsRejected
		m.mu.Unlock()

		if hub != nil {
			clients, accounts := hub.counts()
			up := 0
			if hub.nodeUp.Load() {
				up = 1
			}
			metric("rpcproxy_ws_clients", "gauge", "Connected websocket clients.")
			fmt.Fprintf(&b, "rpcproxy_ws_clients %d\n", clients)
			metric("rpcproxy_ws_subscribed_accounts", "gauge", "Distinct accounts watched by websocket clients.")
			fmt.Fprintf(&b, "rpcproxy_ws_subscribed_accounts %d\n", accounts)
			metric("rpcproxy_ws_connections_total", "counter", "Accepted websocket connections.")
			fmt.Fprintf(&b, "rpcproxy_ws_connections_total %d\n", wsConnects)
			metric("rpcproxy_ws_rejected_total", "counter", "Refused websocket connections (blocked, rate or connection limits).")
			fmt.Fprintf(&b, "rpcproxy_ws_rejected_total %d\n", wsRejected)
			metric("rpcproxy_ws_messages_forwarded_total", "counter", "Confirmation messages sent to clients.")
			fmt.Fprintf(&b, "rpcproxy_ws_messages_forwarded_total %d\n", hub.forwardedTotal.Load())
			metric("rpcproxy_node_websocket_up", "gauge", "1 when connected to the node websocket.")
			fmt.Fprintf(&b, "rpcproxy_node_websocket_up %d\n", up)
		}

		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		metric("rpcproxy_build_info", "gauge", "Version and network of the running proxy.")
		fmt.Fprintf(&b, "rpcproxy_build_info{version=%q,network=%q,go=%q} 1\n", version, cfg.net.Name, runtime.Version())
		metric("rpcproxy_uptime_seconds", "gauge", "Seconds since the proxy started.")
		fmt.Fprintf(&b, "rpcproxy_uptime_seconds %d\n", int64(time.Since(m.started).Seconds()))
		metric("go_goroutines", "gauge", "Number of goroutines.")
		fmt.Fprintf(&b, "go_goroutines %d\n", runtime.NumGoroutine())
		metric("go_memstats_heap_alloc_bytes", "gauge", "Heap bytes in use.")
		fmt.Fprintf(&b, "go_memstats_heap_alloc_bytes %d\n", ms.HeapAlloc)

		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.Write([]byte(b.String()))
	})
}
