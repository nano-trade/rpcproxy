package main

import (
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"
)

// rateLimiter is a token bucket per key (client IP).
type rateLimiter struct {
	mu      sync.Mutex
	rate    float64
	burst   float64
	buckets map[string]*bucket
}

type bucket struct {
	tokens float64
	last   time.Time
}

func newRateLimiter(rate, burst float64) *rateLimiter {
	rl := &rateLimiter{rate: rate, burst: burst, buckets: map[string]*bucket{}}
	go rl.cleanup()
	return rl
}

func (rl *rateLimiter) allow(key string, cost float64) bool {
	now := time.Now()
	rl.mu.Lock()
	defer rl.mu.Unlock()
	b, ok := rl.buckets[key]
	if !ok {
		b = &bucket{tokens: rl.burst, last: now}
		rl.buckets[key] = b
	}
	b.tokens = min(rl.burst, b.tokens+now.Sub(b.last).Seconds()*rl.rate)
	b.last = now
	if b.tokens < cost {
		return false
	}
	b.tokens -= cost
	return true
}

// cleanup drops buckets that have been idle long enough to be full again.
func (rl *rateLimiter) cleanup() {
	for range time.Tick(time.Minute) {
		cutoff := time.Now().Add(-time.Duration(rl.burst/rl.rate*float64(time.Second)) - time.Minute)
		rl.mu.Lock()
		for k, b := range rl.buckets {
			if b.last.Before(cutoff) {
				delete(rl.buckets, k)
			}
		}
		rl.mu.Unlock()
	}
}

// clientIP returns the caller's address. The real-IP header is only trusted
// when the direct peer is a trusted proxy (nginx on localhost), so callers
// cannot spoof it by sending the header themselves.
func clientIP(r *http.Request, c *Config) netip.Addr {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	peer, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}
	}
	peer = peer.Unmap()
	if c.RealIPHeader == "" || !inPrefixes(peer, c.trusted) {
		return peer
	}
	v := strings.TrimSpace(strings.Split(r.Header.Get(c.RealIPHeader), ",")[0])
	if a, err := netip.ParseAddr(v); err == nil {
		return a.Unmap()
	}
	return peer
}

func inPrefixes(a netip.Addr, list []netip.Prefix) bool {
	for _, p := range list {
		if p.Contains(a) {
			return true
		}
	}
	return false
}
