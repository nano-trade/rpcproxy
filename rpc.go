package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const maxNodeResponse = 32 << 20

var reAction = regexp.MustCompile(`^[a-z_]{1,64}$`)

type stats struct {
	requests, rateLimited, rejected, nodeErrors, cacheHits atomic.Int64
}

type Proxy struct {
	cfg     *Config
	client  *http.Client
	limiter *rateLimiter
	cache   *cache
	sem     chan struct{}
	stats   stats
	metrics *metrics
}

func newProxy(cfg *Config, limiter *rateLimiter, m *metrics) *Proxy {
	return &Proxy{
		cfg:     cfg,
		limiter: limiter,
		metrics: m,
		cache:   newCache(),
		sem:     make(chan struct{}, cfg.MaxConcurrentRequests),
		client: &http.Client{
			Timeout: time.Duration(cfg.RPCTimeoutSeconds) * time.Second,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: cfg.MaxConcurrentRequests,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

type httpError struct {
	status int
	msg    string
}

func (e *httpError) Error() string { return e.msg }

func errorf(status int, format string, a ...any) error {
	return &httpError{status, fmt.Sprintf(format, a...)}
}

func (p *Proxy) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(p.cfg.RequestPath, p.handleRPC)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintf(w, "%s RPC proxy. Send requests to %s\n", p.cfg.net.Name, p.cfg.RequestPath)
	})
	return p.securityHeaders(mux)
}

func (p *Proxy) securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
		if origin := r.Header.Get("Origin"); origin != "" && p.cfg.originAllowed(origin) {
			if p.cfg.anyOrigin {
				h.Set("Access-Control-Allow-Origin", "*")
			} else {
				h.Set("Access-Control-Allow-Origin", origin)
				h.Add("Vary", "Origin")
			}
			h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			h.Set("Access-Control-Allow-Headers", "Content-Type")
			h.Set("Access-Control-Expose-Headers", "X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset, Retry-After")
			h.Set("Access-Control-Max-Age", "86400")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (p *Proxy) handleRPC(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	p.stats.requests.Add(1)
	ip := clientIP(r, p.cfg)
	action, status, result := "", http.StatusOK, resultOK
	defer func() {
		p.metrics.request(p.actionLabel(action), result)
		if p.cfg.LogRequests {
			slog.Info("rpc", "ip", ip.String(), "action", action, "status", status, "ms", time.Since(start).Milliseconds())
		}
	}()

	fail := func(err error) {
		status = http.StatusInternalServerError
		var he *httpError
		if errors.As(err, &he) {
			status = he.status
		}
		switch status {
		case http.StatusTooManyRequests:
			p.stats.rateLimited.Add(1)
			result = resultRateLimited
		case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			p.stats.nodeErrors.Add(1)
			result = resultNodeError
		default:
			p.stats.rejected.Add(1)
			result = resultRejected
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
	}

	if !ip.IsValid() || inPrefixes(ip, p.cfg.blocked) {
		fail(errorf(http.StatusForbidden, "IP has been blocked"))
		return
	}
	ok, remaining, reset := p.limiter.allow(ip.String(), 1)
	setRateLimitHeaders(w.Header(), p.cfg.RateLimit, remaining, reset)
	if !ok {
		fail(errorf(http.StatusTooManyRequests, "You are making requests too fast, please slow down!"))
		return
	}

	params, err := p.parseParams(w, r)
	if err != nil {
		fail(err)
		return
	}
	action, _ = params["action"].(string)
	body, err := p.dispatch(r.Context(), action, params)
	if err != nil {
		fail(err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(body)
}

// setRateLimitHeaders tells clients their budget, like NanoRPCProxy did.
// Limit is the burst size, Reset the seconds until the bucket is full again.
func setRateLimitHeaders(h http.Header, rl RateLimit, remaining float64, reset time.Duration) {
	h.Set("X-RateLimit-Limit", strconv.Itoa(int(rl.Burst)))
	h.Set("X-RateLimit-Remaining", strconv.Itoa(int(remaining)))
	h.Set("X-RateLimit-Reset", strconv.Itoa(int(math.Ceil(reset.Seconds()))))
	if remaining < 1 {
		h.Set("Retry-After", strconv.Itoa(int(math.Ceil((1-remaining)/rl.PerSecond))))
	}
}

// actionLabel keeps the metrics label set bounded to the allowlist.
func (p *Proxy) actionLabel(action string) string {
	if p.cfg.allowed[action] {
		return action
	}
	if action == "" {
		return "_none"
	}
	return "_other"
}

// mentionsBlockedAccount reports whether the request's account or accounts
// parameter names an account from blocked_accounts.
func (p *Proxy) mentionsBlockedAccount(params map[string]any) bool {
	if len(p.cfg.blockedAccounts) == 0 {
		return false
	}
	check := func(v any) bool {
		s, ok := v.(string)
		return ok && p.cfg.blockedAccounts[p.cfg.net.canonical(strings.ToLower(s))]
	}
	if check(params["account"]) {
		return true
	}
	if arr, ok := params["accounts"].([]any); ok {
		for _, v := range arr {
			if check(v) {
				return true
			}
		}
	}
	return false
}

// parseParams reads GET query parameters or a JSON POST body.
func (p *Proxy) parseParams(w http.ResponseWriter, r *http.Request) (map[string]any, error) {
	switch r.Method {
	case http.MethodGet:
		return queryParams(r.URL.Query()), nil
	case http.MethodPost:
		r.Body = http.MaxBytesReader(w, r.Body, p.cfg.MaxBodyBytes)
		dec := json.NewDecoder(r.Body)
		dec.UseNumber()
		var params map[string]any
		if err := dec.Decode(&params); err != nil {
			var mbe *http.MaxBytesError
			if errors.As(err, &mbe) {
				return nil, errorf(http.StatusRequestEntityTooLarge, "Request body too large")
			}
			return nil, errorf(http.StatusBadRequest, "Body must be a JSON object")
		}
		if dec.More() || params == nil {
			return nil, errorf(http.StatusBadRequest, "Body must be a single JSON object")
		}
		return params, nil
	default:
		return nil, errorf(http.StatusMethodNotAllowed, "Use GET or POST")
	}
}

// queryParams turns ?a=1&accounts[]=x&accounts[]=y into {"a":"1","accounts":["x","y"]},
// matching what the old Express proxy accepted.
func queryParams(q url.Values) map[string]any {
	out := make(map[string]any, len(q))
	for k, vs := range q {
		name, isArray := strings.CutSuffix(k, "[]")
		if isArray || len(vs) > 1 {
			arr := make([]any, len(vs))
			for i, v := range vs {
				arr[i] = v
			}
			out[name] = arr
		} else {
			out[name] = vs[0]
		}
	}
	return out
}

func (p *Proxy) dispatch(ctx context.Context, action string, params map[string]any) ([]byte, error) {
	if !reAction.MatchString(action) || !p.cfg.allowed[action] {
		if action == "" {
			return nil, errorf(http.StatusBadRequest, "No action provided")
		}
		if len(action) > 64 {
			action = action[:64]
		}
		return nil, errorf(http.StatusForbidden, "Action %s not allowed", action)
	}
	for k, v := range params {
		if arr, ok := v.([]any); ok && len(arr) > p.cfg.MaxArrayItems {
			return nil, errorf(http.StatusBadRequest, "Too many items in %s (max %d)", k, p.cfg.MaxArrayItems)
		}
	}
	if p.mentionsBlockedAccount(params) {
		return nil, errorf(http.StatusForbidden, "Account not allowed")
	}

	switch action {
	case "price":
		return p.price()
	case "mnano_to_raw", "mnano_from_raw":
		amount, ok := params["amount"].(string)
		if !ok {
			if n, isNum := params["amount"].(json.Number); isNum {
				amount, ok = n.String(), true
			}
		}
		if !ok {
			return nil, errorf(http.StatusBadRequest, "Amount not provided!")
		}
		conv := toRaw
		if action == "mnano_from_raw" {
			conv = fromRaw
		}
		res, valid := conv(amount, p.cfg.net.Decimals)
		if !valid {
			res = "N/A"
		}
		return json.Marshal(map[string]string{"amount": res})
	}

	if max, ok := p.cfg.LimitedActions[action]; ok {
		if n, err := strconv.Atoi(fmt.Sprint(params["count"])); err != nil || n > max || n < 1 {
			params["count"] = strconv.Itoa(max)
		}
	}
	if ttl, ok := p.cfg.CachedActions[action]; ok && ttl > 0 {
		key, _ := json.Marshal(sortedParams(params))
		body, hit, err := p.cache.get(string(key), time.Duration(ttl)*time.Second, func() ([]byte, bool, error) {
			// detached context: the result is shared with other waiting clients
			ctx, cancel := context.WithTimeout(context.Background(), p.client.Timeout)
			defer cancel()
			b, err := p.callNode(ctx, params)
			return b, err == nil && !bytes.Contains(b, []byte(`"error"`)), err
		})
		if hit {
			p.stats.cacheHits.Add(1)
			p.metrics.add(&p.metrics.cacheHits)
		}
		return body, err
	}
	return p.callNode(ctx, params)
}

// sortedParams gives a stable cache key regardless of parameter order.
func sortedParams(m map[string]any) [][2]any {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([][2]any, len(keys))
	for i, k := range keys {
		out[i] = [2]any{k, m[k]}
	}
	return out
}

func (p *Proxy) callNode(ctx context.Context, params map[string]any) ([]byte, error) {
	select {
	case p.sem <- struct{}{}:
		defer func() { <-p.sem }()
	case <-time.After(5 * time.Second):
		return nil, errorf(http.StatusServiceUnavailable, "Server busy, try again")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	payload, err := json.Marshal(params)
	if err != nil {
		return nil, errorf(http.StatusBadRequest, "Invalid parameters")
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, p.cfg.NodeRPCURL, bytes.NewReader(payload))
	for k, v := range p.cfg.NodeHeaders {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")
	start := time.Now()
	resp, err := p.client.Do(req)
	action, _ := params["action"].(string)
	p.metrics.nodeCall(action, time.Since(start))
	if err != nil {
		slog.Warn("node request failed", "err", err)
		return nil, errorf(http.StatusBadGateway, "Node unavailable")
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxNodeResponse+1))
	if err != nil {
		return nil, errorf(http.StatusBadGateway, "Node unavailable")
	}
	if len(body) > maxNodeResponse {
		return nil, errorf(http.StatusBadGateway, "Node response too large, request less data")
	}
	if !json.Valid(body) {
		slog.Warn("node returned invalid JSON", "status", resp.StatusCode)
		return nil, errorf(http.StatusBadGateway, "Invalid response from node")
	}
	return body, nil
}

func (p *Proxy) price() ([]byte, error) {
	if p.cfg.PriceURL == "" {
		return nil, errorf(http.StatusNotFound, "Price is not configured")
	}
	ttl := time.Duration(max(p.cfg.PriceCacheSeconds, 1)) * time.Second
	body, hit, err := p.cache.get("\x00price", ttl, func() ([]byte, bool, error) {
		// own timeout: the result is shared, so it must not depend on one client's request
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, p.cfg.PriceURL, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, false, errorf(http.StatusBadGateway, "Price service unavailable")
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if err != nil || resp.StatusCode != http.StatusOK || !json.Valid(b) {
			return nil, false, errorf(http.StatusBadGateway, "Price service unavailable")
		}
		return b, true, nil
	})
	if hit {
		p.stats.cacheHits.Add(1)
		p.metrics.add(&p.metrics.cacheHits)
	}
	return body, err
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
