package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"strings"
)

// Network holds the per-coin constants. Banano and Nano share the same node
// software and RPC, they only differ in address prefix and unit size.
type Network struct {
	Name     string
	Prefixes []string // valid address prefixes, e.g. "ban_"
	Decimals int      // raw digits in one whole coin (Banano 29, Nano 30)
}

var networks = map[string]Network{
	"banano": {Name: "banano", Prefixes: []string{"ban_"}, Decimals: 29},
	"nano":   {Name: "nano", Prefixes: []string{"nano_", "xrb_"}, Decimals: 30},
}

type RateLimit struct {
	PerSecond float64 `json:"per_second"` // sustained requests per second per IP
	Burst     float64 `json:"burst"`      // short bursts allowed above the sustained rate
}

type WSConfig struct {
	Listen                   string `json:"listen"` // empty disables the websocket server
	NodeURL                  string `json:"node_url"`
	MaxConnections           int    `json:"max_connections"`
	MaxConnectionsPerIP      int    `json:"max_connections_per_ip"`
	MaxAccountsPerMessage    int    `json:"max_accounts_per_message"`
	MaxAccountsPerConnection int    `json:"max_accounts_per_connection"`
}

type Config struct {
	Network               string            `json:"network"`
	Listen                string            `json:"listen"`
	RequestPath           string            `json:"request_path"`
	NodeRPCURL            string            `json:"node_rpc_url"`
	TrustedProxies        []string          `json:"trusted_proxies"`
	RealIPHeader          string            `json:"real_ip_header"`
	AllowedActions        []string          `json:"allowed_actions"`
	CachedActions         map[string]int    `json:"cached_actions"`  // action -> seconds
	LimitedActions        map[string]int    `json:"limited_actions"` // action -> max "count"
	MaxArrayItems         int               `json:"max_array_items"`
	MaxBodyBytes          int64             `json:"max_body_bytes"`
	RPCTimeoutSeconds     int               `json:"rpc_timeout_seconds"`
	MaxConcurrentRequests int               `json:"max_concurrent_node_requests"`
	RateLimit             RateLimit         `json:"rate_limit"`
	CORSOrigins           []string          `json:"cors_origins"` // empty = any origin
	NodeHeaders           map[string]string `json:"node_headers"` // extra headers for the node, e.g. auth for a remote node
	BlockedAccounts       []string          `json:"blocked_accounts"`
	MetricsListen         string            `json:"metrics_listen"` // Prometheus /metrics, empty disables it
	PriceURL              string            `json:"price_url"`
	PriceCacheSeconds     int               `json:"price_cache_seconds"`
	IPBlocklist           []string          `json:"ip_blocklist"`
	LogRequests           bool              `json:"log_requests"`
	WS                    WSConfig          `json:"websocket"`

	// Deprecated: the node ignores watch_work since V22. Accepted so old configs still load.
	DisableWatchWork *bool `json:"disable_watch_work,omitempty"`

	// derived
	net             Network
	allowed         map[string]bool
	trusted         []netip.Prefix
	blocked         []netip.Prefix
	anyOrigin       bool
	origins         map[string]bool
	blockedAccounts map[string]bool
}

func defaultConfig() Config {
	return Config{
		Network:               "banano",
		Listen:                "127.0.0.1:9950",
		RequestPath:           "/proxy",
		NodeRPCURL:            "http://[::1]:7072",
		TrustedProxies:        []string{"127.0.0.1/32", "::1/128"},
		RealIPHeader:          "X-Real-IP",
		MaxArrayItems:         500,
		MaxBodyBytes:          256 << 10,
		RPCTimeoutSeconds:     15,
		MaxConcurrentRequests: 64,
		RateLimit:             RateLimit{PerSecond: 20, Burst: 40},
		PriceCacheSeconds:     60,
		WS: WSConfig{
			MaxConnections:           5000,
			MaxConnectionsPerIP:      20,
			MaxAccountsPerMessage:    100,
			MaxAccountsPerConnection: 500,
		},
	}
}

func loadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	c := defaultConfig()
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields() // catch typos in the config
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &c, c.prepare()
}

func (c *Config) prepare() error {
	n, ok := networks[c.Network]
	if !ok {
		return fmt.Errorf("network must be \"banano\" or \"nano\", got %q", c.Network)
	}
	c.net = n
	if c.Listen == "" || c.NodeRPCURL == "" {
		return errors.New("listen and node_rpc_url are required")
	}
	if !strings.HasPrefix(c.RequestPath, "/") {
		c.RequestPath = "/" + c.RequestPath
	}
	if len(c.AllowedActions) == 0 {
		return errors.New("allowed_actions is empty")
	}
	c.allowed = map[string]bool{}
	for _, a := range c.AllowedActions {
		c.allowed[a] = true
	}
	var err error
	if c.trusted, err = parsePrefixes(c.TrustedProxies); err != nil {
		return fmt.Errorf("trusted_proxies: %w", err)
	}
	if c.blocked, err = parsePrefixes(c.IPBlocklist); err != nil {
		return fmt.Errorf("ip_blocklist: %w", err)
	}
	c.blockedAccounts = map[string]bool{}
	for _, a := range c.BlockedAccounts {
		if !validAddress(a, c.net) {
			return fmt.Errorf("blocked_accounts: %q is not a valid %s address", a, c.net.Name)
		}
		c.blockedAccounts[c.net.canonical(a)] = true
	}
	c.anyOrigin = len(c.CORSOrigins) == 0
	c.origins = map[string]bool{}
	for _, o := range c.CORSOrigins {
		c.origins[strings.TrimRight(o, "/")] = true
	}
	if c.RateLimit.PerSecond <= 0 || c.RateLimit.Burst < 1 {
		return errors.New("rate_limit.per_second and rate_limit.burst must be positive")
	}
	if c.MaxConcurrentRequests < 1 || c.MaxArrayItems < 1 || c.MaxBodyBytes < 1024 || c.RPCTimeoutSeconds < 1 {
		return errors.New("max_concurrent_node_requests, max_array_items, max_body_bytes and rpc_timeout_seconds must be positive")
	}
	if c.WS.Listen != "" && c.WS.NodeURL == "" {
		return errors.New("websocket.node_url is required when websocket.listen is set")
	}
	return nil
}

// parsePrefixes accepts plain IPs ("1.2.3.4") and CIDR ranges ("10.0.0.0/8").
func parsePrefixes(list []string) ([]netip.Prefix, error) {
	var out []netip.Prefix
	for _, s := range list {
		if strings.Contains(s, "/") {
			p, err := netip.ParsePrefix(s)
			if err != nil {
				return nil, err
			}
			out = append(out, p.Masked())
			continue
		}
		a, err := netip.ParseAddr(s)
		if err != nil {
			return nil, err
		}
		a = a.Unmap()
		out = append(out, netip.PrefixFrom(a, a.BitLen()))
	}
	return out, nil
}

// canonical returns the address with the network's main prefix (nano_, not xrb_).
func (n Network) canonical(addr string) string {
	if i := strings.IndexByte(addr, '_'); i >= 0 && addr[:i+1] != n.Prefixes[0] {
		return n.Prefixes[0] + addr[i+1:]
	}
	return addr
}

func (c *Config) originAllowed(origin string) bool {
	return c.anyOrigin || c.origins[strings.TrimRight(origin, "/")]
}
