package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"opencode-go-cliproxyapi/internal/errclass"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

const (
	// DefaultProbeInterval is the baseline periodic quota probe interval (30 minutes).
	DefaultProbeInterval = 30 * time.Minute
	// LongRangeProbeInterval is used when reset is far away (>6 hours) to reduce probe noise.
	LongRangeProbeInterval = 1 * time.Hour
	// LongRangeThreshold defines when to switch to the longer probe interval (6 hours).
	LongRangeThreshold = 6 * time.Hour
	// ProbeGracePeriod is added to upstream reset time to prevent race conditions at exact reset boundaries.
	ProbeGracePeriod = 5 * time.Second
	// ProbeRequestTimeout bounds quota probe HTTP requests.
	ProbeRequestTimeout = 10 * time.Second
	// ProbeFailureBackoff is the backoff duration when upstream /usage probe fails.
	ProbeFailureBackoff = 5 * time.Minute
)

// keyBreakerRecord tracks the in-memory circuit breaker state for a specific API Key.
type keyBreakerRecord struct {
	key         string
	trippedAt   time.Time
	resetTime   time.Time // Upstream reset timestamp if known
	nextProbeAt time.Time // Next time when quota should be actively probed
	probing     bool      // Single-flight guard during active probe
	lastReason  string
}

// CircuitBreaker provides in-memory circuit breaking per API key to prevent
// upstream request penetration when quota is exhausted.
type CircuitBreaker struct {
	mu        sync.RWMutex
	records   map[string]*keyBreakerRecord
	bridge    *HostBridge
	baseURLFn func() string
	nowFn     func() time.Time // For deterministic testing
}

// NewCircuitBreaker creates a new circuit breaker instance.
func NewCircuitBreaker(bridge *HostBridge, baseURLFn func() string) *CircuitBreaker {
	return &CircuitBreaker{
		records:   make(map[string]*keyBreakerRecord),
		bridge:    bridge,
		baseURLFn: baseURLFn,
		nowFn:     time.Now,
	}
}

func (b *CircuitBreaker) now() time.Time {
	if b.nowFn != nil {
		return b.nowFn()
	}
	return time.Now()
}

// Trip trips the circuit breaker for an API key upon quota exhaustion (429).
func (b *CircuitBreaker) Trip(key string, reason string, body []byte) {
	if b == nil || strings.TrimSpace(key) == "" {
		return
	}
	now := b.now()
	resetTime := extractResetTime(reason, body)

	b.mu.Lock()
	defer b.mu.Unlock()

	nextProbeAt := b.calculateNextProbeTime(now, resetTime)
	record, exists := b.records[key]
	if !exists {
		record = &keyBreakerRecord{
			key: key,
		}
		b.records[key] = record
	}
	record.trippedAt = now
	record.resetTime = resetTime
	record.nextProbeAt = nextProbeAt
	record.lastReason = reason
	record.probing = false

	debugTrace("circuit breaker tripped for key=%s resetTime=%s nextProbeAt=%s reason=%s",
		redactKeySnippet(key),
		formatTimeOrNone(resetTime),
		nextProbeAt.Format(time.RFC3339),
		reason,
	)
}

// calculateNextProbeTime implements the adaptive tiered probe schedule based on
// remaining reset duration:
// 1. Tier 1 (remaining <= 30m): Align directly to resetTime + 5s (instant recovery at boundary).
// 2. Tier 2 (30m < remaining <= 6h): Probe every 30 minutes (high sensitivity for manual top-ups).
// 3. Tier 3 (remaining > 6h): Probe every 1 hour (reduces unnecessary probe traffic during multi-day waits).
// 4. Default (no reset time): Fall back to baseline 30 minutes.
func (b *CircuitBreaker) calculateNextProbeTime(now time.Time, resetTime time.Time) time.Time {
	if !resetTime.IsZero() && resetTime.After(now) {
		remaining := resetTime.Sub(now)
		// Tier 1: Near reset (<= 30 minutes) -> align to reset boundary + grace
		if remaining <= DefaultProbeInterval {
			return resetTime.Add(ProbeGracePeriod)
		}
		// Tier 2: Mid-range (30m to 6h) -> 30 minutes
		if remaining <= LongRangeThreshold {
			return now.Add(DefaultProbeInterval)
		}
		// Tier 3: Long-range (> 6h) -> 1 hour
		probeAt := now.Add(LongRangeProbeInterval)
		if probeAt.After(resetTime) {
			return resetTime.Add(ProbeGracePeriod)
		}
		return probeAt
	}
	return now.Add(DefaultProbeInterval)
}

// Check checks whether requests using this API key should be short-circuited.
// If quota is exhausted and not yet recovered, it returns a ClassQuota error.
// If the reset time has arrived, the breaker automatically closes (Rule 2).
// If the 30-minute (or adaptive) probe window has arrived, it probes /usage (Rule 1).
func (b *CircuitBreaker) Check(ctx context.Context, key string) *errclass.Error {
	if b == nil || strings.TrimSpace(key) == "" {
		return nil
	}
	now := b.now()

	b.mu.Lock()
	record, exists := b.records[key]
	if !exists {
		b.mu.Unlock()
		return nil
	}

	// Rule 2: If reset time has arrived, automatically close circuit breaker.
	if !record.resetTime.IsZero() && !now.Before(record.resetTime) {
		debugTrace("circuit breaker reset time arrived for key=%s, clearing breaker", redactKeySnippet(key))
		delete(b.records, key)
		b.mu.Unlock()
		return nil
	}

	// Check if probe window has arrived
	if now.Before(record.nextProbeAt) {
		// Still in cooldown period; short-circuit immediately without upstream I/O
		msg := b.formatBlockedMessage(record, now)
		b.mu.Unlock()
		return &errclass.Error{
			Class:      errclass.ClassQuota,
			Message:    msg,
			StatusCode: http.StatusTooManyRequests,
			Retryable:  true,
		}
	}

	// Probe window reached (Rule 1)
	if record.probing {
		// Another probe is currently in progress; short-circuit without extra I/O
		msg := b.formatBlockedMessage(record, now)
		b.mu.Unlock()
		return &errclass.Error{
			Class:      errclass.ClassQuota,
			Message:    msg,
			StatusCode: http.StatusTooManyRequests,
			Retryable:  true,
		}
	}

	// Mark probing = true to guard single-flight
	record.probing = true
	b.mu.Unlock()

	// Execute active quota probe
	restored, newResetTime, err := b.probeQuota(ctx, key)

	b.mu.Lock()
	defer b.mu.Unlock()

	rec, ok := b.records[key]
	if !ok {
		return nil
	}
	rec.probing = false

	if err == nil && restored {
		debugTrace("circuit breaker probe succeeded: quota restored for key=%s", redactKeySnippet(key))
		delete(b.records, key)
		return nil
	}

	// Quota is still exhausted or probe failed; reschedule probe
	if !newResetTime.IsZero() {
		rec.resetTime = newResetTime
	}
	if err != nil {
		debugTrace("circuit breaker probe failed for key=%s: %v, backing off", redactKeySnippet(key), err)
		rec.nextProbeAt = b.now().Add(ProbeFailureBackoff)
	} else {
		rec.nextProbeAt = b.calculateNextProbeTime(b.now(), rec.resetTime)
		debugTrace("circuit breaker probe: quota still exhausted for key=%s, nextProbeAt=%s",
			redactKeySnippet(key), rec.nextProbeAt.Format(time.RFC3339))
	}

	msg := b.formatBlockedMessage(rec, b.now())
	return &errclass.Error{
		Class:      errclass.ClassQuota,
		Message:    msg,
		StatusCode: http.StatusTooManyRequests,
		Retryable:  true,
	}
}

// probeQuota performs an upstream /usage request to check whether quota has been restored.
func (b *CircuitBreaker) probeQuota(ctx context.Context, key string) (restored bool, resetTime time.Time, err error) {
	if b.bridge == nil || b.baseURLFn == nil {
		return false, time.Time{}, fmt.Errorf("probe bridge or baseURL unavailable")
	}
	baseURL := b.baseURLFn()
	if baseURL == "" {
		return false, time.Time{}, fmt.Errorf("baseURL is empty")
	}

	if ctx == nil {
		ctx = context.Background()
	}
	probeCtx, cancel := context.WithTimeout(ctx, ProbeRequestTimeout)
	defer cancel()

	url := strings.TrimRight(baseURL, "/") + "/usage"
	resp, err := b.bridge.Do(probeCtx, pluginapi.HTTPRequest{
		Method: http.MethodGet,
		URL:    url,
		Headers: http.Header{
			"Authorization": {"Bearer " + key},
			"Accept":        {"application/json"},
		},
	})
	if err != nil {
		return false, time.Time{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return false, time.Time{}, fmt.Errorf("upstream probe returned HTTP %d", resp.StatusCode)
	}

	return parseQuotaUsageRecovery(resp.Body)
}

// parseQuotaUsageRecovery parses /usage response body to check if quota is restored.
// If any window (e.g. weekly or monthly) has percent >= 100, quota is still exhausted.
// If all windows have percent < 100, quota is considered restored.
func parseQuotaUsageRecovery(body []byte) (restored bool, resetTime time.Time, err error) {
	var decoded struct {
		Usage map[string]struct {
			Percent  *float64 `json:"percent"`
			ResetsAt string   `json:"resetsAt"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return false, time.Time{}, fmt.Errorf("invalid quota json: %w", err)
	}
	if len(decoded.Usage) == 0 {
		return false, time.Time{}, fmt.Errorf("no usage windows found")
	}

	exhausted := false
	var earliestReset time.Time

	for _, window := range decoded.Usage {
		if window.Percent != nil && *window.Percent >= 100 {
			exhausted = true
			if window.ResetsAt != "" {
				if rTime, rErr := time.Parse(time.RFC3339Nano, window.ResetsAt); rErr == nil {
					if earliestReset.IsZero() || rTime.Before(earliestReset) {
						earliestReset = rTime
					}
				} else if rTime, rErr := time.Parse(time.RFC3339, window.ResetsAt); rErr == nil {
					if earliestReset.IsZero() || rTime.Before(earliestReset) {
						earliestReset = rTime
					}
				}
			}
		}
	}

	return !exhausted, earliestReset, nil
}

var (
	resetOnRe      = regexp.MustCompile(`(?i)resets\s+(?:on|at)\s+([0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?(?:\s*[A-Za-z0-9_+\-:]+)?)`)
	jsonResetsAtRe = regexp.MustCompile(`"resetsAt"\s*:\s*"([^"]+)"`)
)

func extractResetTime(reason string, body []byte) time.Time {
	if len(body) > 0 {
		if match := jsonResetsAtRe.FindSubmatch(body); len(match) > 1 {
			if t, err := parseResetTimeHelper(string(match[1])); err == nil {
				return t
			}
		}
	}
	text := reason + " " + string(body)
	if match := resetOnRe.FindStringSubmatch(text); len(match) > 1 {
		raw := strings.TrimSpace(match[1])
		if t, err := parseResetTimeHelper(raw); err == nil {
			return t
		}
	}
	return time.Time{}
}

func parseResetTimeHelper(raw string) (time.Time, error) {
	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05 MST",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, raw); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unknown reset time format: %s", raw)
}

func (b *CircuitBreaker) formatBlockedMessage(record *keyBreakerRecord, now time.Time) string {
	var parts []string
	parts = append(parts, "OpenCode API key quota exhausted (circuit breaker active)")
	if !record.resetTime.IsZero() {
		parts = append(parts, fmt.Sprintf("resets at %s", record.resetTime.Format(time.RFC3339)))
	}
	if !record.nextProbeAt.IsZero() {
		remain := record.nextProbeAt.Sub(now).Round(time.Second)
		if remain > 0 {
			parts = append(parts, fmt.Sprintf("next probe in %s", remain))
		}
	}
	return strings.Join(parts, "; ")
}

func redactKeySnippet(key string) string {
	if len(key) <= 8 {
		return "[redacted]"
	}
	return key[:4] + "..." + key[len(key)-4:]
}

func formatTimeOrNone(t time.Time) string {
	if t.IsZero() {
		return "none"
	}
	return t.Format(time.RFC3339)
}
