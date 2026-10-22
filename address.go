package main

import (
	"bytes"
	"math/big"
	"strings"

	"golang.org/x/crypto/blake2b"
)

const addrAlphabet = "13456789abcdefghijkmnopqrstuwxyz"

var addrIndex = func() (m [256]int8) {
	for i := range m {
		m[i] = -1
	}
	for i := 0; i < len(addrAlphabet); i++ {
		m[addrAlphabet[i]] = int8(i)
	}
	return
}()

func decodeBase32(s string) (*big.Int, bool) {
	n := new(big.Int)
	for i := 0; i < len(s); i++ {
		v := addrIndex[s[i]]
		if v < 0 {
			return nil, false
		}
		n.Lsh(n, 5)
		n.Or(n, big.NewInt(int64(v)))
	}
	return n, true
}

// validAddress checks prefix, length, alphabet and the blake2b checksum.
func validAddress(addr string, n Network) bool {
	var body string
	for _, p := range n.Prefixes {
		if strings.HasPrefix(addr, p) {
			body = addr[len(p):]
			break
		}
	}
	if len(body) != 60 || (body[0] != '1' && body[0] != '3') {
		return false
	}
	key, ok := decodeBase32(body[:52])
	if !ok || key.BitLen() > 256 {
		return false
	}
	sum, ok := decodeBase32(body[52:])
	if !ok {
		return false
	}
	pub := key.FillBytes(make([]byte, 32))
	h, _ := blake2b.New(5, nil)
	h.Write(pub)
	want := h.Sum(nil)
	for i, j := 0, len(want)-1; i < j; i, j = i+1, j-1 {
		want[i], want[j] = want[j], want[i]
	}
	return bytes.Equal(sum.FillBytes(make([]byte, 5)), want)
}
