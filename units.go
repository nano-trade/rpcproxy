package main

import (
	"math/big"
	"regexp"
	"strings"
)

var (
	reDecimal = regexp.MustCompile(`^[0-9]{1,40}(\.[0-9]{1,40})?$`)
	reRaw     = regexp.MustCompile(`^[0-9]{1,60}$`)
)

// toRaw converts a whole-coin amount ("1.5") to raw units.
func toRaw(amount string, decimals int) (string, bool) {
	if !reDecimal.MatchString(amount) {
		return "", false
	}
	whole, frac, _ := strings.Cut(amount, ".")
	if len(frac) > decimals {
		return "", false
	}
	n, ok := new(big.Int).SetString(whole+frac+strings.Repeat("0", decimals-len(frac)), 10)
	if !ok {
		return "", false
	}
	return n.String(), true
}

// fromRaw converts raw units to a whole-coin amount, without trailing zeros.
func fromRaw(raw string, decimals int) (string, bool) {
	if !reRaw.MatchString(raw) {
		return "", false
	}
	n, _ := new(big.Int).SetString(raw, 10)
	s := n.String()
	if len(s) <= decimals {
		s = strings.Repeat("0", decimals-len(s)+1) + s
	}
	whole, frac := s[:len(s)-decimals], strings.TrimRight(s[len(s)-decimals:], "0")
	if frac == "" {
		return whole, true
	}
	return whole + "." + frac, true
}
