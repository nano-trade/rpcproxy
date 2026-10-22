package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAddresses(t *testing.T) {
	ban, nano := networks["banano"], networks["nano"]
	good := []string{
		"ban_1bananobh5rat99qfgt1ptpieie5swmoth87thi74qgbfrij7dcgjiij94xr",
		"ban_19potasho7ozny8r1drz3u3hb3r97fw4ndm4hegdsdzzns1c3nobdastcgaa",
		"ban_34sce59x8zca8heegpwwkig3r87upqtfgxahmqtpupirs1yhkgwk739aqc8z",
	}
	for _, a := range good {
		if !validAddress(a, ban) {
			t.Errorf("%s should be valid", a)
		}
		if validAddress(a, nano) {
			t.Errorf("%s must not be valid on nano", a)
		}
		// same key with nano prefixes is valid on nano
		if !validAddress("nano_"+a[4:], nano) || !validAddress("xrb_"+a[4:], nano) {
			t.Errorf("nano/xrb form of %s should be valid", a)
		}
	}
	bad := []string{
		"", "ban_", "ban_1bananobh5rat99qfgt1ptpieie5swmoth87thi74qgbfrij7dcgjiij94xx", // checksum
		"ban_1bananobh5rat99qfgt1ptpieie5swmoth87thi74qgbfrij7dcgjiij94x",  // short
		"ban_2bananobh5rat99qfgt1ptpieie5swmoth87thi74qgbfrij7dcgjiij94xr", // bad first char
		"ban_1bananobh5rat99qfgt1ptpieie5swmoth87thi74qgbfrij7dcgjiij94x0", // bad alphabet
		"BAN_1bananobh5rat99qfgt1ptpieie5swmoth87thi74qgbfrij7dcgjiij94xr",
	}
	for _, a := range bad {
		if validAddress(a, ban) {
			t.Errorf("%q should be invalid", a)
		}
	}
}

func TestUnits(t *testing.T) {
	cases := []struct {
		coin string
		dec  int
		raw  string
	}{
		{"1", 29, "100000000000000000000000000000"},
		{"1.5", 29, "150000000000000000000000000000"},
		{"0.00000000000000000000000000001", 29, "1"},
		{"1", 30, "1000000000000000000000000000000"},
		{"0", 29, "0"},
	}
	for _, c := range cases {
		if got, ok := toRaw(c.coin, c.dec); !ok || got != c.raw {
			t.Errorf("toRaw(%s,%d) = %s %v, want %s", c.coin, c.dec, got, ok, c.raw)
		}
		if got, ok := fromRaw(c.raw, c.dec); !ok || got != c.coin {
			t.Errorf("fromRaw(%s,%d) = %s %v, want %s", c.raw, c.dec, got, ok, c.coin)
		}
	}
	for _, s := range []string{"", "-1", "1.", ".5", "1e5", "abc", "0." + strings.Repeat("0", 29) + "1"} {
		if _, ok := toRaw(s, 29); ok {
			t.Errorf("toRaw(%q) should fail", s)
		}
	}
	if _, ok := fromRaw("1.5", 29); ok {
		t.Error("fromRaw must reject decimals")
	}
}

// fakeNode records what the proxy sends and answers like a node would.
type fakeNode struct {
	mu      sync.Mutex
	got     []map[string]any
	headers http.Header
	calls   atomic.Int64
	delay   time.Duration
}

func (f *fakeNode) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.calls.Add(1)
	time.Sleep(f.delay)
	var m map[string]any
	json.NewDecoder(r.Body).Decode(&m)
	f.mu.Lock()
	f.got = append(f.got, m)
	f.headers = r.Header.Clone()
	f.mu.Unlock()
	switch m["action"] {
	case "block_count":
		w.Write([]byte(`{"count":"100","unchecked":"0","cemented":"100"}`))
	case "account_balance":
		w.Write([]byte(`{"balance":"1","pending":"0","receivable":"0"}`))
	default:
		w.Write([]byte(`{"ok":"1"}`))
	}
}

func (f *fakeNode) last() map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.got[len(f.got)-1]
}

func testProxy(t *testing.T, node http.Handler, mod func(*Config)) (*httptest.Server, *Config) {
	t.Helper()
	ns := httptest.NewServer(node)
	t.Cleanup(ns.Close)
	c := defaultConfig()
	c.NodeRPCURL = ns.URL
	c.AllowedActions = []string{"block_count", "account_balance", "accounts_balances", "account_history", "process", "mnano_to_raw", "mnano_from_raw"}
	c.CachedActions = map[string]int{"block_count": 30}
	c.LimitedActions = map[string]int{"account_history": 50}
	c.RateLimit = RateLimit{PerSecond: 1000, Burst: 1000}
	if mod != nil {
		mod(&c)
	}
	if err := c.prepare(); err != nil {
		t.Fatal(err)
	}
	ps := httptest.NewServer(newProxy(&c, newRateLimiter(c.RateLimit.PerSecond, c.RateLimit.Burst), newMetrics()).routes())
	t.Cleanup(ps.Close)
	return ps, &c
}

func get(t *testing.T, url string, hdr ...string) (int, string, http.Header) {
	t.Helper()
	req, _ := http.NewRequest("GET", url, nil)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), resp.Header
}

func post(t *testing.T, url, body string) (int, string) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestRPC(t *testing.T) {
	node := &fakeNode{}
	ps, _ := testProxy(t, node, nil)
	u := ps.URL + "/proxy"

	t.Run("GET passthrough", func(t *testing.T) {
		code, body, h := get(t, u+"?action=account_balance&account=ban_x")
		if code != 200 || !strings.Contains(body, `"balance":"1"`) || h.Get("Content-Type") != "application/json" {
			t.Fatalf("got %d %s", code, body)
		}
		if node.last()["account"] != "ban_x" {
			t.Fatalf("node got %v", node.last())
		}
	})
	t.Run("POST passthrough", func(t *testing.T) {
		code, body := post(t, u, `{"action":"accounts_balances","accounts":["a","b"]}`)
		if code != 200 || !strings.Contains(body, `"ok"`) {
			t.Fatalf("got %d %s", code, body)
		}
		if a, _ := node.last()["accounts"].([]any); len(a) != 2 {
			t.Fatalf("node got %v", node.last())
		}
	})
	t.Run("GET arrays", func(t *testing.T) {
		get(t, u+"?action=accounts_balances&accounts[]=a&accounts[]=b&accounts[]=c")
		if a, _ := node.last()["accounts"].([]any); len(a) != 3 {
			t.Fatalf("node got %v", node.last())
		}
	})
	t.Run("disallowed action", func(t *testing.T) {
		before := node.calls.Load()
		for _, q := range []string{"?action=wallet_create", "?action=stop", "", "?action=BLOCK_COUNT"} {
			code, body, _ := get(t, u+q)
			if code < 400 || !strings.Contains(body, "error") {
				t.Errorf("%s: got %d %s", q, code, body)
			}
		}
		if code, _ := post(t, u, `{"action":"key_expand","key":"x"}`); code != 403 {
			t.Errorf("post disallowed: %d", code)
		}
		if node.calls.Load() != before {
			t.Error("disallowed actions reached the node")
		}
	})
	t.Run("bad bodies", func(t *testing.T) {
		for _, b := range []string{`[]`, `"x"`, `{"action":"block_count"}{"a":1}`, `{bad`, ``} {
			if code, _ := post(t, u, b); code != 400 {
				t.Errorf("%q: got %d", b, code)
			}
		}
		big := `{"action":"block_count","x":"` + strings.Repeat("a", 300<<10) + `"}`
		if code, _ := post(t, u, big); code != 413 {
			t.Errorf("big body: got %d", code)
		}
	})
	t.Run("array limit", func(t *testing.T) {
		accts := strings.TrimSuffix(strings.Repeat(`"a",`, 501), ",")
		if code, _ := post(t, u, `{"action":"accounts_balances","accounts":[`+accts+`]}`); code != 400 {
			t.Errorf("got %d", code)
		}
	})
	t.Run("count limit", func(t *testing.T) {
		for _, q := range []string{"&count=100000", "", "&count=-5", "&count=abc"} {
			get(t, u+"?action=account_history&account=a"+q)
			if node.last()["count"] != "50" {
				t.Errorf("%s: node got count %v", q, node.last()["count"])
			}
		}
		get(t, u+"?action=account_history&account=a&count=10")
		if node.last()["count"] != "10" {
			t.Errorf("small count changed: %v", node.last()["count"])
		}
	})
	t.Run("units", func(t *testing.T) {
		_, body, _ := get(t, u+"?action=mnano_to_raw&amount=1")
		if !strings.Contains(body, `"100000000000000000000000000000"`) {
			t.Errorf("banano 1 BAN = 10^29 raw, got %s", body)
		}
		_, body = post(t, u, `{"action":"mnano_from_raw","amount":"150000000000000000000000000000"}`)
		if !strings.Contains(body, `"1.5"`) {
			t.Errorf("got %s", body)
		}
		_, body, _ = get(t, u+"?action=mnano_to_raw&amount=xyz")
		if !strings.Contains(body, `"N/A"`) {
			t.Errorf("got %s", body)
		}
	})
	t.Run("cache and coalescing", func(t *testing.T) {
		node.delay = 100 * time.Millisecond
		defer func() { node.delay = 0 }()
		before := node.calls.Load()
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if code, body, _ := get(t, u+"?action=block_count"); code != 200 || !strings.Contains(body, `"count"`) {
					t.Errorf("got %d %s", code, body)
				}
			}()
		}
		wg.Wait()
		get(t, u+"?action=block_count")
		if n := node.calls.Load() - before; n != 1 {
			t.Errorf("node called %d times for cached action, want 1", n)
		}
	})
	t.Run("cors", func(t *testing.T) {
		req, _ := http.NewRequest("OPTIONS", u, nil)
		req.Header.Set("Origin", "https://nault.banano.trade")
		resp, _ := http.DefaultClient.Do(req)
		resp.Body.Close()
		if resp.StatusCode != 204 || resp.Header.Get("Access-Control-Allow-Origin") != "*" {
			t.Errorf("preflight: %d %v", resp.StatusCode, resp.Header)
		}
	})
	t.Run("root and unknown paths", func(t *testing.T) {
		if code, _, _ := get(t, ps.URL+"/"); code != 200 {
			t.Error("root")
		}
		if code, _, _ := get(t, ps.URL+"/api.php"); code != 404 {
			t.Error("unknown path should 404")
		}
	})
}

func TestNodeDown(t *testing.T) {
	ps, c := testProxy(t, &fakeNode{}, nil)
	c.NodeRPCURL = "http://127.0.0.1:1" // nothing listens there
	code, body, _ := get(t, ps.URL+"/proxy?action=account_balance&account=a")
	if code != 502 || !strings.Contains(body, "Node unavailable") {
		t.Fatalf("got %d %s", code, body)
	}
}

func TestRateLimitAndRealIP(t *testing.T) {
	ps, _ := testProxy(t, &fakeNode{}, func(c *Config) {
		c.RateLimit = RateLimit{PerSecond: 0.001, Burst: 3}
	})
	u := ps.URL + "/proxy?action=account_balance&account=a"
	// the test client connects from 127.0.0.1, a trusted proxy, so X-Real-IP is used
	for i := 0; i < 3; i++ {
		if code, _, _ := get(t, u, "X-Real-IP", "203.0.113.1"); code != 200 {
			t.Fatalf("request %d: %d", i, code)
		}
	}
	if code, _, h := get(t, u, "X-Real-IP", "203.0.113.1"); code != 429 || h.Get("Retry-After") == "" {
		t.Fatalf("4th request should be limited, got %d", code)
	}
	// a different client is unaffected
	if code, _, _ := get(t, u, "X-Real-IP", "203.0.113.2"); code != 200 {
		t.Fatalf("other IP limited: %d", code)
	}
}

func TestSpoofedHeaderIgnored(t *testing.T) {
	c := defaultConfig()
	c.trusted, _ = parsePrefixes([]string{"10.0.0.1"})
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "198.51.100.7:5555"
	r.Header.Set("X-Real-IP", "1.2.3.4")
	if ip := clientIP(r, &c); ip.String() != "198.51.100.7" {
		t.Fatalf("untrusted peer could spoof its IP: %s", ip)
	}
	r.RemoteAddr = "10.0.0.1:5555"
	if ip := clientIP(r, &c); ip.String() != "1.2.3.4" {
		t.Fatalf("trusted proxy header ignored: %s", ip)
	}
}

func TestBlocklist(t *testing.T) {
	ps, _ := testProxy(t, &fakeNode{}, func(c *Config) { c.IPBlocklist = []string{"203.0.113.0/24"} })
	if code, _, _ := get(t, ps.URL+"/proxy?action=block_count", "X-Real-IP", "203.0.113.9"); code != 403 {
		t.Fatalf("blocked IP got %d", code)
	}
}

func TestBlockedAccounts(t *testing.T) {
	const burn = "nano_1111111111111111111111111111111111111111111111111111hifc8npp"
	node := &fakeNode{}
	ps, _ := testProxy(t, node, func(c *Config) {
		c.Network = "nano"
		c.AllowedActions = append(c.AllowedActions, "receivable", "accounts_receivable")
		c.BlockedAccounts = []string{burn}
	})
	u := ps.URL + "/proxy"
	before := node.calls.Load()
	for _, q := range []string{
		"?action=receivable&account=" + burn,
		"?action=receivable&account=xrb_1111111111111111111111111111111111111111111111111111hifc8npp",
		"?action=account_balance&account=NANO_1111111111111111111111111111111111111111111111111111HIFC8NPP",
	} {
		if code, body, _ := get(t, u+q); code != 403 || !strings.Contains(body, "Account not allowed") {
			t.Errorf("%s: got %d %s", q, code, body)
		}
	}
	if code, _ := post(t, u, `{"action":"accounts_receivable","accounts":["nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3","`+burn+`"]}`); code != 403 {
		t.Errorf("accounts list with burn: %d", code)
	}
	if node.calls.Load() != before {
		t.Error("blocked requests reached the node")
	}
	if code, _, _ := get(t, u+"?action=receivable&account=nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3"); code != 200 {
		t.Errorf("normal account refused: %d", code)
	}
	c := defaultConfig()
	c.AllowedActions = []string{"x"}
	c.BlockedAccounts = []string{"ban_notanaddress"}
	if c.prepare() == nil {
		t.Error("invalid blocked account accepted")
	}
}

func TestRateLimitHeaders(t *testing.T) {
	ps, _ := testProxy(t, &fakeNode{}, func(c *Config) { c.RateLimit = RateLimit{PerSecond: 1, Burst: 2} })
	u := ps.URL + "/proxy?action=block_count"
	_, _, h := get(t, u, "X-Real-IP", "198.51.100.1")
	if h.Get("X-RateLimit-Limit") != "2" || h.Get("X-RateLimit-Remaining") != "1" || h.Get("X-RateLimit-Reset") != "1" {
		t.Fatalf("headers after 1st request: %v", h)
	}
	get(t, u, "X-Real-IP", "198.51.100.1")
	code, _, h := get(t, u, "X-Real-IP", "198.51.100.1")
	if code != 429 || h.Get("X-RateLimit-Remaining") != "0" || h.Get("Retry-After") != "1" {
		t.Fatalf("3rd request: %d %v", code, h)
	}
}

func TestNodeHeadersAndMetrics(t *testing.T) {
	node := &fakeNode{}
	ns := httptest.NewServer(node)
	defer ns.Close()
	c := defaultConfig()
	c.NodeRPCURL = ns.URL
	c.AllowedActions = []string{"block_count", "account_balance"}
	c.CachedActions = map[string]int{"block_count": 30}
	c.NodeHeaders = map[string]string{"Authorization": "Bearer secret"}
	if err := c.prepare(); err != nil {
		t.Fatal(err)
	}
	m := newMetrics()
	ps := httptest.NewServer(newProxy(&c, newRateLimiter(1000, 1000), m).routes())
	defer ps.Close()
	get(t, ps.URL+"/proxy?action=block_count")
	get(t, ps.URL+"/proxy?action=block_count")
	get(t, ps.URL+"/proxy?action=wallet_create")
	node.mu.Lock()
	auth := node.headers.Get("Authorization")
	node.mu.Unlock()
	if auth != "Bearer secret" {
		t.Errorf("node headers not sent, got %q", auth)
	}

	ms := httptest.NewServer(m.handler(&c, nil))
	defer ms.Close()
	_, body, h := get(t, ms.URL+"/metrics")
	for _, want := range []string{
		`rpcproxy_requests_total{action="block_count",result="ok"} 2`,
		`rpcproxy_requests_total{action="_other",result="rejected"} 1`,
		`rpcproxy_cache_hits_total 1`,
		`rpcproxy_node_request_seconds_count{action="block_count"} 1`,
		`rpcproxy_build_info{`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
	if strings.Contains(body, "wallet_create") {
		t.Error("disallowed action leaked into metric labels")
	}
	if !strings.HasPrefix(h.Get("Content-Type"), "text/plain") {
		t.Error("wrong content type")
	}
}

func TestOldConfigStillLoads(t *testing.T) {
	f := t.TempDir() + "/c.json"
	os.WriteFile(f, []byte(`{"allowed_actions":["block_count"],"disable_watch_work":true}`), 0o600)
	if _, err := loadConfig(f); err != nil {
		t.Fatalf("config with obsolete disable_watch_work rejected: %v", err)
	}
	os.WriteFile(f, []byte(`{"allowed_actions":["block_count"],"alowed_typo":1}`), 0o600)
	if _, err := loadConfig(f); err == nil {
		t.Fatal("unknown key accepted")
	}
}
