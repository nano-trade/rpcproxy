package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

const (
	acctA = "ban_1bananobh5rat99qfgt1ptpieie5swmoth87thi74qgbfrij7dcgjiij94xr"
	acctB = "ban_19potasho7ozny8r1drz3u3hb3r97fw4ndm4hegdsdzzns1c3nobdastcgaa"
	acctC = "ban_34sce59x8zca8heegpwwkig3r87upqtfgxahmqtpupirs1yhkgwk739aqc8z"
)

// fakeNodeWS tracks the node-side subscription the proxy maintains.
type fakeNodeWS struct {
	mu       sync.Mutex
	conn     *websocket.Conn
	accounts map[string]bool
	changed  chan struct{}
}

func (f *fakeNodeWS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
	if err != nil {
		return
	}
	f.mu.Lock()
	f.conn = c
	f.mu.Unlock()
	for {
		var m struct {
			Action  string `json:"action"`
			Options struct {
				Accounts    []string `json:"accounts"`
				AccountsAdd []string `json:"accounts_add"`
				AccountsDel []string `json:"accounts_del"`
			} `json:"options"`
		}
		if c.ReadJSON(&m) != nil {
			return
		}
		f.mu.Lock()
		switch m.Action {
		case "subscribe":
			f.accounts = map[string]bool{}
			for _, a := range m.Options.Accounts {
				f.accounts[a] = true
			}
		case "update":
			for _, a := range m.Options.AccountsAdd {
				f.accounts[a] = true
			}
			for _, a := range m.Options.AccountsDel {
				delete(f.accounts, a)
			}
		}
		f.mu.Unlock()
		f.changed <- struct{}{}
	}
}

func (f *fakeNodeWS) waitFor(t *testing.T, want ...string) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		f.mu.Lock()
		ok := len(f.accounts) == len(want)
		for _, a := range want {
			ok = ok && f.accounts[a]
		}
		got := len(f.accounts)
		f.mu.Unlock()
		if ok {
			return
		}
		select {
		case <-f.changed:
		case <-deadline:
			t.Fatalf("node subscription has %d accounts, want %v", got, want)
		}
	}
}

func (f *fakeNodeWS) confirm(t *testing.T, from, to string) {
	t.Helper()
	msg := `{"topic":"confirmation","time":"1","message":{"account":"` + from + `","amount":"1","hash":"H","confirmation_type":"active_quorum","block":{"type":"state","account":"` + from + `","link_as_account":"` + to + `","subtype":"send"}}}`
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.conn.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
		t.Fatal(err)
	}
}

func dial(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	c, _, err := websocket.DefaultDialer.Dial(url, http.Header{"Origin": {"https://nault.banano.trade"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func readMsg(t *testing.T, c *websocket.Conn) map[string]any {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	var m map[string]any
	if err := c.ReadJSON(&m); err != nil {
		t.Fatal(err)
	}
	return m
}

// expectNothing checks no message is queued: the next one must be our pong.
func expectNothing(t *testing.T, c *websocket.Conn) {
	t.Helper()
	c.WriteMessage(websocket.TextMessage, []byte(`{"action":"ping"}`))
	if m := readMsg(t, c); m["ack"] != "pong" {
		t.Fatalf("unexpected message: %v", m)
	}
}

func TestWebsocket(t *testing.T) {
	node := &fakeNodeWS{changed: make(chan struct{}, 100)}
	ns := httptest.NewServer(node)
	defer ns.Close()

	c := defaultConfig()
	c.AllowedActions = []string{"block_count"}
	c.WS.Listen = "x"
	c.WS.NodeURL = "ws" + strings.TrimPrefix(ns.URL, "http")
	c.WS.MaxAccountsPerConnection = 2
	c.WS.MaxConnectionsPerIP = 3
	c.RateLimit = RateLimit{PerSecond: 1000, Burst: 1000}
	if err := c.prepare(); err != nil {
		t.Fatal(err)
	}
	hub := newHub(&c, newRateLimiter(1000, 1000), newMetrics())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.runNode(ctx)
	hs := httptest.NewServer(hub)
	defer hs.Close()
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")
	node.waitFor(t) // connected, empty subscription

	alice, bob := dial(t, wsURL), dial(t, wsURL)
	sub := func(c *websocket.Conn, accts ...string) map[string]any {
		b, _ := json.Marshal(map[string]any{"action": "subscribe", "topic": "confirmation", "options": map[string]any{"accounts": accts}})
		c.WriteMessage(websocket.TextMessage, b)
		return readMsg(t, c)
	}

	if m := sub(alice, acctA, "ban_invalid"); m["ack"] != "subscribe" {
		t.Fatalf("got %v", m)
	}
	if m := sub(bob, acctB); m["ack"] != "subscribe" {
		t.Fatalf("got %v", m)
	}
	node.waitFor(t, acctA, acctB)

	// send from A to C: only alice watches A
	node.confirm(t, acctA, acctC)
	if m := readMsg(t, alice); m["topic"] != "confirmation" {
		t.Fatalf("alice got %v", m)
	}
	expectNothing(t, bob)

	// send from C to B: bob is the receiver
	node.confirm(t, acctC, acctB)
	if m := readMsg(t, bob); m["topic"] != "confirmation" {
		t.Fatalf("bob got %v", m)
	}
	expectNothing(t, alice)

	// per-connection account cap
	if m := sub(alice, acctB, acctC); m["error"] == nil {
		t.Fatalf("cap not enforced: %v", m)
	}
	// missing options
	alice.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","topic":"confirmation"}`))
	if m := readMsg(t, alice); m["error"] == nil {
		t.Fatalf("got %v", m)
	}

	// bob leaves: B is dropped from the node subscription, A stays
	bob.Close()
	node.waitFor(t, acctA)

	// unsubscribe removes the rest
	alice.WriteMessage(websocket.TextMessage, []byte(`{"action":"unsubscribe","topic":"confirmation","id":7}`))
	if m := readMsg(t, alice); m["ack"] != "unsubscribe" || m["id"] != float64(7) {
		t.Fatalf("unsubscribe ack: %v", m)
	}
	node.waitFor(t)

	// per-IP connection limit (alice is still open, so 2 more fit, the 4th fails)
	dial(t, wsURL)
	dial(t, wsURL)
	if _, resp, err := websocket.DefaultDialer.Dial(wsURL, nil); err == nil || resp == nil || resp.StatusCode != 429 {
		t.Fatalf("per-IP limit not enforced: %v", err)
	}

	// node restart: the proxy reconnects and re-subscribes
	sub(alice, acctC)
	node.waitFor(t, acctC)
	node.mu.Lock()
	node.accounts = map[string]bool{} // forget state, so waitFor needs a fresh subscribe
	node.conn.Close()
	node.mu.Unlock()
	node.waitFor(t, acctC)
	node.confirm(t, acctC, acctA)
	if m := readMsg(t, alice); m["topic"] != "confirmation" {
		t.Fatalf("after reconnect alice got %v", m)
	}
}

// TestWebsocketUpdate covers the "update" action in both styles (node's
// accounts_add/accounts_del and NanoRPCProxy's options.accounts) and ack ids.
func TestWebsocketUpdate(t *testing.T) {
	node := &fakeNodeWS{changed: make(chan struct{}, 100)}
	ns := httptest.NewServer(node)
	defer ns.Close()
	c := defaultConfig()
	c.AllowedActions = []string{"block_count"}
	c.WS.Listen = "x"
	c.WS.NodeURL = "ws" + strings.TrimPrefix(ns.URL, "http")
	c.WS.MaxAccountsPerConnection = 2
	if err := c.prepare(); err != nil {
		t.Fatal(err)
	}
	hub := newHub(&c, newRateLimiter(1000, 1000), newMetrics())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.runNode(ctx)
	hs := httptest.NewServer(hub)
	defer hs.Close()
	node.waitFor(t)
	ws := dial(t, "ws"+strings.TrimPrefix(hs.URL, "http"))
	send := func(v any) map[string]any {
		b, _ := json.Marshal(v)
		ws.WriteMessage(websocket.TextMessage, b)
		return readMsg(t, ws)
	}

	m := send(map[string]any{"action": "subscribe", "topic": "confirmation", "id": "abc", "options": map[string]any{"accounts": []string{acctA}}})
	if m["ack"] != "subscribe" || m["id"] != "abc" || m["time"] == nil {
		t.Fatalf("subscribe ack: %v", m)
	}
	// node style: swap A for B
	m = send(map[string]any{"action": "update", "topic": "confirmation", "options": map[string]any{"accounts_add": []string{acctB}, "accounts_del": []string{acctA}}})
	if m["ack"] != "update" {
		t.Fatalf("update ack: %v", m)
	}
	node.waitFor(t, acctB)
	// NanoRPCProxy style: options.accounts adds
	send(map[string]any{"action": "update", "topic": "confirmation", "options": map[string]any{"accounts": []string{acctC}}})
	node.waitFor(t, acctB, acctC)
	// a swap at the cap is fine, growing past it is not
	if m = send(map[string]any{"action": "update", "topic": "confirmation", "options": map[string]any{"accounts_add": []string{acctA}, "accounts_del": []string{acctC}}}); m["ack"] != "update" {
		t.Fatalf("swap at cap refused: %v", m)
	}
	if m = send(map[string]any{"action": "update", "topic": "confirmation", "options": map[string]any{"accounts_add": []string{acctC}}}); m["error"] == nil {
		t.Fatalf("cap not enforced on update: %v", m)
	}
	node.waitFor(t, acctA, acctB)
	if m = send(map[string]any{"action": "ping", "id": 5}); m["ack"] != "pong" || m["id"] != float64(5) {
		t.Fatalf("ping: %v", m)
	}
}
