// rpcproxy is a small, hardened public proxy for Banano and Nano node RPC
// and websocket confirmations.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var version = "dev"

func main() {
	configPath := flag.String("config", "config.json", "path to the JSON config file")
	check := flag.Bool("check", false, "validate the config and exit")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, nil)))
	cfg, err := loadConfig(*configPath)
	if err != nil {
		slog.Error("bad config", "err", err)
		os.Exit(1)
	}
	if *check {
		slog.Info("config ok", "network", cfg.Network)
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	limiter := newRateLimiter(cfg.RateLimit.PerSecond, cfg.RateLimit.Burst)
	proxy := newProxy(cfg, limiter)
	servers := []*http.Server{newServer(cfg.Listen, proxy.routes())}

	var hub *Hub
	if cfg.WS.Listen != "" {
		hub = newHub(cfg, limiter)
		go hub.runNode(ctx)
		servers = append(servers, newServer(cfg.WS.Listen, hub))
	}

	slog.Info("starting", "version", version, "network", cfg.Network, "rpc", cfg.Listen,
		"websocket", cfg.WS.Listen, "node", cfg.NodeRPCURL, "actions", len(cfg.AllowedActions))

	errc := make(chan error, len(servers))
	for _, s := range servers {
		go func(s *http.Server) {
			if err := s.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				errc <- err
			}
		}(s)
	}

	go logStats(ctx, proxy, hub)

	select {
	case err := <-errc:
		slog.Error("server failed", "err", err)
		os.Exit(1)
	case <-ctx.Done():
	}
	slog.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, s := range servers {
		s.Shutdown(sctx)
	}
}

func newServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       20 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
}

// logStats prints a one-line summary every 10 minutes.
func logStats(ctx context.Context, p *Proxy, h *Hub) {
	t := time.NewTicker(10 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		args := []any{
			"requests", p.stats.requests.Swap(0),
			"rate_limited", p.stats.rateLimited.Swap(0),
			"rejected", p.stats.rejected.Swap(0),
			"node_errors", p.stats.nodeErrors.Swap(0),
			"cache_hits", p.stats.cacheHits.Swap(0),
		}
		if h != nil {
			clients, accounts := h.counts()
			args = append(args, "ws_clients", clients, "ws_accounts", accounts,
				"ws_forwarded", h.forwarded.Swap(0), "node_ws_up", h.nodeUp.Load())
		}
		slog.Info("stats (last 10 min)", args...)
	}
}
