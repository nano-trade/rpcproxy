package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	wsMaxMessage = 64 << 10
	wsPongWait   = 90 * time.Second
	wsPingEvery  = 30 * time.Second
	wsWriteWait  = 10 * time.Second
	wsSendBuffer = 256
	nodeRetryMin = time.Second
	nodeRetryMax = 15 * time.Second
)

// Hub relays confirmations from the node websocket to subscribed clients.
// It keeps one connection to the node, subscribed to the union of all
// accounts clients asked for, and fans each confirmation out to the
// clients watching the sender or the receiver.
type Hub struct {
	cfg     *Config
	limiter *rateLimiter

	mu         sync.Mutex
	clients    map[*wsClient]struct{}
	perIP      map[netip.Addr]int
	subs       map[string]map[*wsClient]struct{}
	pendingAdd map[string]struct{}
	pendingDel map[string]struct{}
	notify     chan struct{}

	nodeUp    atomic.Bool
	forwarded atomic.Int64
}

type wsClient struct {
	hub      *Hub
	conn     *websocket.Conn
	ip       netip.Addr
	send     chan []byte
	done     chan struct{}       // closed when the client is gone
	accounts map[string]struct{} // guarded by hub.mu
	once     sync.Once
}

func newHub(cfg *Config, limiter *rateLimiter) *Hub {
	return &Hub{
		cfg: cfg, limiter: limiter,
		clients:    map[*wsClient]struct{}{},
		perIP:      map[netip.Addr]int{},
		subs:       map[string]map[*wsClient]struct{}{},
		pendingAdd: map[string]struct{}{},
		pendingDel: map[string]struct{}{},
		notify:     make(chan struct{}, 1),
	}
}

// ---- client side ----

func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ip := clientIP(r, h.cfg)
	if !ip.IsValid() || inPrefixes(ip, h.cfg.blocked) {
		http.Error(w, "blocked", http.StatusForbidden)
		return
	}
	if !h.limiter.allow(ip.String(), 1) {
		http.Error(w, "too many requests", http.StatusTooManyRequests)
		return
	}
	h.mu.Lock()
	if len(h.clients) >= h.cfg.WS.MaxConnections || h.perIP[ip] >= h.cfg.WS.MaxConnectionsPerIP {
		h.mu.Unlock()
		http.Error(w, "too many connections", http.StatusTooManyRequests)
		return
	}
	h.perIP[ip]++ // reserve the slot before the (slow) upgrade
	h.mu.Unlock()

	up := websocket.Upgrader{
		ReadBufferSize: 4096, WriteBufferSize: 4096,
		CheckOrigin: func(r *http.Request) bool {
			o := r.Header.Get("Origin")
			return o == "" || h.cfg.originAllowed(o)
		},
	}
	conn, err := up.Upgrade(w, r, nil)
	if err != nil {
		h.mu.Lock()
		h.releaseIP(ip)
		h.mu.Unlock()
		return
	}
	c := &wsClient{hub: h, conn: conn, ip: ip, send: make(chan []byte, wsSendBuffer), done: make(chan struct{}), accounts: map[string]struct{}{}}
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
	go c.writeLoop()
	c.readLoop()
}

func (h *Hub) releaseIP(ip netip.Addr) {
	if h.perIP[ip]--; h.perIP[ip] <= 0 {
		delete(h.perIP, ip)
	}
}

func (c *wsClient) close() {
	c.once.Do(func() {
		h := c.hub
		h.mu.Lock()
		for a := range c.accounts {
			h.unsubLocked(c, a)
		}
		delete(h.clients, c)
		h.releaseIP(c.ip)
		h.mu.Unlock()
		close(c.done)
		c.conn.Close()
	})
}

func (c *wsClient) readLoop() {
	defer c.close()
	c.conn.SetReadLimit(wsMaxMessage)
	c.conn.SetReadDeadline(time.Now().Add(wsPongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(wsPongWait))
	})
	for {
		typ, data, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		c.conn.SetReadDeadline(time.Now().Add(wsPongWait))
		if typ != websocket.TextMessage {
			continue
		}
		if !c.hub.limiter.allow(c.ip.String(), 1) {
			c.reply(map[string]string{"error": "You are making requests too fast, please slow down!"})
			continue
		}
		c.handle(data)
	}
}

type wsRequest struct {
	Action  string `json:"action"`
	Topic   string `json:"topic"`
	Options *struct {
		Accounts []any `json:"accounts"`
	} `json:"options"`
}

func (c *wsClient) handle(data []byte) {
	var m wsRequest
	if json.Unmarshal(data, &m) != nil {
		return // old proxy silently ignored garbage too
	}
	h := c.hub
	switch m.Action {
	case "ping":
		c.reply(map[string]any{"ack": "pong", "time": time.Now().UnixMilli()})
	case "subscribe":
		if m.Topic != "confirmation" {
			return
		}
		if m.Options == nil || m.Options.Accounts == nil {
			c.reply(map[string]string{"error": "You must provide the accounts to track via the options parameter"})
			return
		}
		if len(m.Options.Accounts) > h.cfg.WS.MaxAccountsPerMessage {
			c.reply(map[string]string{"error": "Too many accounts subscribed"})
			return
		}
		valid := h.normalizeAccounts(m.Options.Accounts)
		h.mu.Lock()
		added := 0
		for _, a := range valid {
			if _, ok := c.accounts[a]; !ok {
				added++
			}
		}
		if len(c.accounts)+added > h.cfg.WS.MaxAccountsPerConnection {
			h.mu.Unlock()
			c.reply(map[string]string{"error": "Too many accounts subscribed"})
			return
		}
		for _, a := range valid {
			h.subLocked(c, a)
		}
		h.mu.Unlock()
		c.reply(map[string]string{"ack": "subscribe"})
	case "unsubscribe":
		if m.Topic != "confirmation" {
			return
		}
		h.mu.Lock()
		if m.Options != nil && len(m.Options.Accounts) > 0 {
			for _, a := range h.normalizeAccounts(m.Options.Accounts) {
				h.unsubLocked(c, a)
			}
		} else {
			for a := range c.accounts {
				h.unsubLocked(c, a)
			}
		}
		h.mu.Unlock()
	}
}

// normalizeAccounts keeps valid addresses only, using the node's prefix (nano_, not xrb_).
func (h *Hub) normalizeAccounts(in []any) []string {
	out := make([]string, 0, len(in))
	for _, v := range in {
		s, ok := v.(string)
		if !ok || !validAddress(s, h.cfg.net) {
			continue
		}
		if p := h.cfg.net.Prefixes[0]; !strings.HasPrefix(s, p) {
			s = p + s[strings.IndexByte(s, '_')+1:]
		}
		out = append(out, s)
	}
	return out
}

func (h *Hub) subLocked(c *wsClient, a string) {
	if _, ok := c.accounts[a]; ok {
		return
	}
	c.accounts[a] = struct{}{}
	set := h.subs[a]
	if set == nil {
		set = map[*wsClient]struct{}{}
		h.subs[a] = set
		if _, ok := h.pendingDel[a]; ok {
			delete(h.pendingDel, a)
		} else {
			h.pendingAdd[a] = struct{}{}
		}
		h.kick()
	}
	set[c] = struct{}{}
}

func (h *Hub) unsubLocked(c *wsClient, a string) {
	if _, ok := c.accounts[a]; !ok {
		return
	}
	delete(c.accounts, a)
	set := h.subs[a]
	delete(set, c)
	if len(set) == 0 {
		delete(h.subs, a)
		if _, ok := h.pendingAdd[a]; ok {
			delete(h.pendingAdd, a)
		} else {
			h.pendingDel[a] = struct{}{}
		}
		h.kick()
	}
}

func (h *Hub) kick() {
	select {
	case h.notify <- struct{}{}:
	default:
	}
}

func (c *wsClient) reply(v any) {
	b, _ := json.Marshal(v)
	c.enqueue(b)
}

// enqueue never blocks; a client that cannot keep up is disconnected.
func (c *wsClient) enqueue(b []byte) {
	select {
	case <-c.done:
	case c.send <- b:
	default:
		go c.close()
	}
}

func (c *wsClient) writeLoop() {
	ping := time.NewTicker(wsPingEvery)
	defer ping.Stop()
	for {
		select {
		case <-c.done:
			return
		case b := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
			if err := c.conn.WriteMessage(websocket.TextMessage, b); err != nil {
				go c.close()
				return
			}
		case <-ping.C:
			if err := c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(wsWriteWait)); err != nil {
				go c.close()
				return
			}
		}
	}
}

// ---- node side ----

func (h *Hub) runNode(ctx context.Context) {
	delay := nodeRetryMin
	for ctx.Err() == nil {
		started := time.Now()
		err := h.nodeSession(ctx)
		h.nodeUp.Store(false)
		if ctx.Err() != nil {
			return
		}
		slog.Warn("node websocket disconnected, reconnecting", "err", err, "in", delay)
		if time.Since(started) > time.Minute {
			delay = nodeRetryMin
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return
		}
		delay = min(delay*2, nodeRetryMax)
	}
}

func (h *Hub) nodeSession(ctx context.Context) error {
	dctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	conn, _, err := websocket.DefaultDialer.DialContext(dctx, h.cfg.WS.NodeURL, nil)
	cancel()
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetReadLimit(16 << 20)

	// Full subscription with everything clients currently watch. Pending
	// changes are already part of this snapshot, so they are cleared.
	h.mu.Lock()
	accounts := make([]string, 0, len(h.subs))
	for a := range h.subs {
		accounts = append(accounts, a)
	}
	clear(h.pendingAdd)
	clear(h.pendingDel)
	h.mu.Unlock()
	sub := map[string]any{"action": "subscribe", "topic": "confirmation", "ack": true,
		"options": map[string]any{"all_local_accounts": false, "accounts": accounts}}
	if err := conn.WriteJSON(sub); err != nil {
		return err
	}
	h.nodeUp.Store(true)
	slog.Info("node websocket connected", "accounts", len(accounts))

	errc := make(chan error, 1)
	go func() {
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				errc <- err
				return
			}
			h.fromNode(data)
		}
	}()

	keepalive := time.NewTicker(wsPingEvery)
	defer keepalive.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errc:
			return err
		case <-keepalive.C:
			if err := conn.WriteJSON(map[string]string{"action": "ping"}); err != nil {
				return err
			}
		case <-h.notify:
			h.mu.Lock()
			add, del := keys(h.pendingAdd), keys(h.pendingDel)
			clear(h.pendingAdd)
			clear(h.pendingDel)
			h.mu.Unlock()
			if len(add)+len(del) == 0 {
				continue
			}
			upd := map[string]any{"action": "update", "topic": "confirmation",
				"options": map[string]any{"accounts_add": add, "accounts_del": del}}
			if err := conn.WriteJSON(upd); err != nil {
				return err
			}
		}
	}
}

type nodeMessage struct {
	Topic   string `json:"topic"`
	Message struct {
		Account string `json:"account"`
		Block   struct {
			LinkAsAccount string `json:"link_as_account"`
		} `json:"block"`
	} `json:"message"`
}

func (h *Hub) fromNode(data []byte) {
	var m nodeMessage
	if json.Unmarshal(data, &m) != nil || m.Topic != "confirmation" {
		return
	}
	var targets []*wsClient
	h.mu.Lock()
	for c := range h.subs[m.Message.Account] {
		targets = append(targets, c)
	}
	if l := m.Message.Block.LinkAsAccount; l != m.Message.Account {
		for c := range h.subs[l] {
			targets = append(targets, c)
		}
	}
	h.mu.Unlock()
	seen := map[*wsClient]bool{}
	for _, c := range targets {
		if !seen[c] { // self-sends would otherwise arrive twice
			seen[c] = true
			c.enqueue(data)
			h.forwarded.Add(1)
		}
	}
}

func keys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func (h *Hub) counts() (clients, accounts int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients), len(h.subs)
}
