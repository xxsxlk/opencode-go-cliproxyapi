package plugin

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

func TestCircuitBreaker_AdaptiveNextProbeTime(t *testing.T) {
	cb := NewCircuitBreaker(nil, nil)
	base := time.Date(2026, 10, 27, 22, 0, 0, 0, time.UTC)
	cb.nowFn = func() time.Time { return base }

	// Case 1: No reset time -> default 30 minutes
	p1 := cb.calculateNextProbeTime(base, time.Time{})
	if want := base.Add(30 * time.Minute); !p1.Equal(want) {
		t.Errorf("calculateNextProbeTime(no reset) = %v, want %v", p1, want)
	}

	// Case 2 (Tier 1): Reset time in 10 minutes (<= 30 minutes) -> aligns to reset + 5s
	resetSoon := base.Add(10 * time.Minute)
	p2 := cb.calculateNextProbeTime(base, resetSoon)
	if want := resetSoon.Add(5 * time.Second); !p2.Equal(want) {
		t.Errorf("calculateNextProbeTime(Tier 1, in 10m) = %v, want %v", p2, want)
	}

	// Case 3 (Tier 2): Reset time in 2 hours (30m < remaining <= 6h) -> probes every 30 minutes
	resetMid := base.Add(2 * time.Hour)
	p3 := cb.calculateNextProbeTime(base, resetMid)
	if want := base.Add(30 * time.Minute); !p3.Equal(want) {
		t.Errorf("calculateNextProbeTime(Tier 2, in 2h) = %v, want %v", p3, want)
	}

	// Case 4 (Tier 3): Reset time in 3 days (> 6h) -> probes every 1 hour
	resetFar := base.Add(3 * 24 * time.Hour)
	p4 := cb.calculateNextProbeTime(base, resetFar)
	if want := base.Add(1 * time.Hour); !p4.Equal(want) {
		t.Errorf("calculateNextProbeTime(Tier 3, in 3d) = %v, want %v", p4, want)
	}
}

func TestCircuitBreaker_Rule2_ResetTimeAutoUnlock(t *testing.T) {
	cb := NewCircuitBreaker(nil, nil)
	base := time.Date(2026, 10, 27, 22, 0, 0, 0, time.UTC)
	currentTime := base
	cb.nowFn = func() time.Time { return currentTime }

	key := "oc_sk_test_key_12345"
	body := []byte(`{"error":{"message":"Go usage limit exceeded: weekly limit reached (resets on 2026-10-27 22:15:00 GMT)"}}`)

	cb.Trip(key, "weekly limit reached", body)

	// In 5 minutes (before reset): should be blocked
	currentTime = base.Add(5 * time.Minute)
	if err := cb.Check(context.Background(), key); err == nil {
		t.Fatal("expected error before reset time, got nil")
	} else if err.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429 status code, got %d", err.StatusCode)
	}

	// In 16 minutes (after reset time arrived): Rule 2 -> auto unlock!
	currentTime = base.Add(16 * time.Minute)
	if err := cb.Check(context.Background(), key); err != nil {
		t.Fatalf("expected breaker to automatically close after reset time arrived, got error: %v", err)
	}
}

func TestCircuitBreaker_Rule1_ProbeRestoreAndContinue(t *testing.T) {
	base := time.Date(2026, 10, 27, 22, 0, 0, 0, time.UTC)
	currentTime := base

	var probeStatus int
	var probeBody []byte

	probeCount := 0
	f := &fakeCaller{
		responder: func(method string, payload []byte) ([]byte, error) {
			probeCount++
			return hostOK(pluginapi.HTTPResponse{
				StatusCode: probeStatus,
				Body:       probeBody,
			}), nil
		},
	}
	bridge := NewHostBridge(f.call)

	cb := NewCircuitBreaker(bridge, func() string { return "https://api.opencode.ai/zen/go/v1" })
	cb.nowFn = func() time.Time { return currentTime }

	key := "oc_sk_probe_test_key"
	// Reset in 2 hours (> 30 min) -> nextProbeAt is base + 30 min
	resetFar := base.Add(2 * time.Hour)
	cb.Trip(key, "quota limit", []byte(`{"resetsAt":"`+resetFar.Format(time.RFC3339)+`"}`))

	// 10 minutes in: before 30 min window -> should be blocked without probing
	currentTime = base.Add(10 * time.Minute)
	err := cb.Check(context.Background(), key)
	if err == nil || probeCount != 0 {
		t.Fatalf("want blocked without probe at 10m, got err=%v, probeCount=%d", err, probeCount)
	}

	// 31 minutes in: probe window arrived -> probes /usage
	// Scenario A: Still exhausted (weekly = 100%)
	probeStatus = http.StatusOK
	probeBody = []byte(`{"usage":{"weekly":{"percent":100,"resetsAt":"` + resetFar.Format(time.RFC3339) + `"}}}`)
	currentTime = base.Add(31 * time.Minute)

	err = cb.Check(context.Background(), key)
	if err == nil {
		t.Fatal("expected still blocked because quota is 100%")
	}
	if probeCount != 1 {
		t.Fatalf("expected 1 probe request, got %d", probeCount)
	}

	// 40 minutes in: before next probe window (which is base + 31m + 30m = base + 61m)
	currentTime = base.Add(40 * time.Minute)
	err = cb.Check(context.Background(), key)
	if err == nil {
		t.Fatal("expected still blocked at 40m")
	}
	if probeCount != 1 {
		t.Fatalf("probeCount should remain 1, got %d", probeCount)
	}

	// 62 minutes in: second probe window -> quota restored (weekly = 50%)
	probeStatus = http.StatusOK
	probeBody = []byte(`{"usage":{"weekly":{"percent":50,"resetsAt":"` + resetFar.Format(time.RFC3339) + `"}}}`)
	currentTime = base.Add(62 * time.Minute)

	err = cb.Check(context.Background(), key)
	if err != nil {
		t.Fatalf("expected breaker to close and return nil on restored quota, got %v", err)
	}
	if probeCount != 2 {
		t.Fatalf("expected 2 total probes, got %d", probeCount)
	}

	// Immediately check again: breaker is closed, no probe needed
	err = cb.Check(context.Background(), key)
	if err != nil {
		t.Fatalf("expected breaker clean, got %v", err)
	}
	if probeCount != 2 {
		t.Fatalf("expected probeCount still 2, got %d", probeCount)
	}
}
