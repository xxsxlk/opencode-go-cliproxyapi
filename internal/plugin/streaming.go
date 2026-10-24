package plugin

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"

	"opencode-go-cliproxyapi/internal/adapter/shared"
	"opencode-go-cliproxyapi/internal/catalog"
	"opencode-go-cliproxyapi/internal/errclass"
)

// streamOpenResult is delivered once, including failures after the first
// keep-alive has committed an SSE response (which can no longer be retried).
type streamOpenResult struct {
	id      string
	status  int
	failure *errclass.Error
}

type streamReadResult struct {
	payload []byte
	message string
	closed  bool
	err     error
}

// streamHeartbeat only carries protocol-valid no-content traffic. For OpenAI
// Chat the host wraps each payload in data: itself, so an SSE comment would be
// an invalid JSON data event. Reuse real chunk identity once it is known.
type streamHeartbeat struct {
	format, model, id string
	created           int64
}

func (h *streamHeartbeat) observe(events [][]byte) {
	if h.format != "openai" || h.id != "" {
		return
	}
	for _, event := range events {
		var chunk struct {
			ID      string `json:"id"`
			Model   string `json:"model"`
			Created int64  `json:"created"`
		}
		if json.Unmarshal(event, &chunk) == nil && chunk.ID != "" {
			h.id = chunk.ID
			if chunk.Model != "" {
				h.model = chunk.Model
			}
			if chunk.Created != 0 {
				h.created = chunk.Created
			}
			return
		}
	}
}

func (h streamHeartbeat) payload() []byte {
	switch h.format {
	case "claude":
		return []byte(": keep-alive\n\n")
	case "openai-response":
		// The host's Responses handler requires a JSON data frame before it
		// commits SSE headers; comment-only frames cannot bootstrap it.
		return []byte("event: ping\ndata: {}\n\n")
	case "openai":
		// An empty delta is not a role, content, or terminal chunk. Do not
		// invent a response id while waiting for the upstream's first chunk.
		body, _ := json.Marshal(struct {
			ID      string `json:"id"`
			Object  string `json:"object"`
			Created int64  `json:"created"`
			Model   string `json:"model"`
			Choices []any  `json:"choices"`
		}{h.id, "chat.completion.chunk", h.created, h.model, []any{map[string]any{
			"index": 0, "delta": map[string]any{}, "finish_reason": nil,
		}}})
		return body
	}
	return nil
}

func (m *Manager) streamTask(work func()) {
	m.bridge.inFlight.Add(1)
	go func() {
		defer m.bridge.inFlight.Done()
		work()
	}()
}

// executeStream keeps fast open failures as pre-emission envelopes so the host
// can retry. A slow open is detached only after the keep-alive grace period;
// subsequent failures are delivered on the stream instead of masquerading as
// a successful model response.
func (m *Manager) executeStream(req executorRequest) ([]byte, error) {
	res, fail := m.resolveExecution(req)
	if res == nil {
		return fail, nil
	}
	if m.bridge == nil {
		return classEnvelope(errclass.FromNetwork(context.Canceled)), nil
	}
	sessionID, eErr := resolveOpenCodeSessionID(req)
	if eErr != nil {
		return classEnvelope(eErr), nil
	}
	body, eErr := buildUpstreamRequest(res.rec.Protocol, res.rec.UpstreamID, req.SourceFormat, req.OriginalRequest, res.rec.Thinking, &res.tools)
	if eErr != nil {
		return classEnvelope(eErr), nil
	}
	body = ensureStreamFlag(body)

	// Hold one in-flight reference across the entire open/pump lifetime so a
	// concurrent shutdown cannot return between scheduling and the first host
	// callback. Reconfigure does not cancel already-running requests. The Add
	// happens under the same RLock as the Err()-check (F1): either it lands
	// before shutdown's write-lock teardown (WaitForInFlight then counts it),
	// or the request sees the niled parent and backs off — never in between,
	// which would let shutdown Wait past this request and dlclose under it.
	m.mu.RLock()
	parent := m.streamCtx
	if parent == nil {
		m.mu.RUnlock()
		return classEnvelope(errclass.FromNetwork(context.Canceled)), nil
	}
	m.bridge.inFlight.Add(1)
	m.mu.RUnlock()
	if parent.Err() != nil {
		m.bridge.inFlight.Done()
		return classEnvelope(errclass.FromNetwork(context.Canceled)), nil
	}
	baseCtx, baseCancel := context.WithCancel(parent)
	ctx := baseCtx
	timeoutCancel := func() {}
	if res.cfg.Streaming.MaxDuration > 0 {
		ctx, timeoutCancel = context.WithTimeout(baseCtx, res.cfg.Streaming.MaxDuration)
	}
	finish := func() {
		timeoutCancel()
		baseCancel()
		m.bridge.inFlight.Done()
	}
	handedOff := false
	defer func() {
		if !handedOff {
			finish()
		}
	}()

	opened := make(chan streamOpenResult, 1)
	m.streamTask(func() {
		opened <- m.openStream(ctx, res, sessionID, body)
	})

	// A zero keepalive interval intentionally disables early SSE commit. With
	// it enabled, quick errors still use the normal retryable envelope path.
	var grace <-chan time.Time
	if res.cfg.Streaming.KeepAliveInterval > 0 {
		timer := time.NewTimer(res.cfg.Streaming.KeepAliveInterval)
		defer timer.Stop()
		grace = timer.C
	}
	result, ready := awaitStreamOpen(opened, grace)
	if ready {
		if result.failure != nil {
			return classEnvelope(result.failure), nil
		}
		if result.status >= 400 {
			return classEnvelope(m.streamStatusError(ctx, res, result)), nil
		}
		if ctx.Err() != nil {
			_ = m.bridge.CloseStreamBounded(result.id)
			return classEnvelope(errclass.FromNetwork(ctx.Err())), nil
		}
		handedOff = true
		go func() {
			defer finish()
			m.pumpStream(ctx, req.StreamID, result.id, res, req.SourceFormat)
		}()
	} else {
		handedOff = true
		go func() {
			defer finish()
			m.waitForStream(ctx, baseCancel, opened, req.StreamID, res, req.SourceFormat)
		}()
	}
	return okEnvelope(struct{}{}), nil
}

// If a header result and the heartbeat grace are simultaneously ready,
// prioritize the header result so known HTTP failures retain retry semantics.
func awaitStreamOpen(opened <-chan streamOpenResult, grace <-chan time.Time) (streamOpenResult, bool) {
	select {
	case result := <-opened:
		return result, true
	case <-grace:
		select {
		case result := <-opened:
			return result, true
		default:
			return streamOpenResult{}, false
		}
	}
}

// openStream bounds the response-header phase independently of body reads.
// A delayed start can be canceled without leaving its host-registered stream
// behind: HostBridge.DoStream closes late arrivals after its timeout.
func (m *Manager) openStream(ctx context.Context, res *resolvedExecution, sessionID string, body []byte) streamOpenResult {
	headerCtx, cancel := context.WithTimeout(ctx, res.cfg.Streaming.HeaderTimeout)
	defer cancel()
	status, _, id, err := m.bridge.DoStream(headerCtx, pluginapi.HTTPRequest{
		Method:  http.MethodPost,
		URL:     catalog.JoinUpstreamURL(res.cfg.BaseURL, res.rec.EndpointPath),
		Headers: upstreamAuthHeaders(res.rec.Protocol, res.key, sessionID), Body: body,
	})
	if err != nil {
		return streamOpenResult{failure: errclass.FromNetwork(err)}
	}
	if ctx.Err() != nil {
		if id != "" {
			_ = m.bridge.CloseStreamBounded(id)
		}
		return streamOpenResult{failure: errclass.FromNetwork(ctx.Err())}
	}
	if status >= 400 {
		return streamOpenResult{id: id, status: status}
	}
	if id == "" {
		return streamOpenResult{failure: errclass.FromNetwork(context.Canceled)}
	}
	return streamOpenResult{id: id}
}

// The status is known before reading the optional error body. This runs only
// after the caller has selected the pre-emission or post-emission path, so a
// slow error body cannot change a retryable HTTP failure into a 200 SSE stream.
func (m *Manager) streamStatusError(ctx context.Context, res *resolvedExecution, result streamOpenResult) *errclass.Error {
	if result.id == "" {
		return shared.UpstreamStatusError(result.status, nil)
	}
	defer m.bridge.CloseStreamBounded(result.id)
	budget := min(res.cfg.RequestTimeout, 2*time.Second)
	readCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	body, _, _, _ := m.bridge.StreamReadContext(readCtx, result.id)
	return shared.UpstreamStatusError(result.status, body)
}

// waitForStream commits heartbeats only when the header phase is slow. It
// owns all emissions until openStream completes; the pump then owns them, so
// heartbeat and real content can never race or reorder.
func (m *Manager) waitForStream(ctx context.Context, cancel context.CancelFunc, opened <-chan streamOpenResult, downID string, res *resolvedExecution, sourceFormat string) {
	// The opening result may have arrived after executeStream's last check but
	// before this goroutine runs. Avoid committing a needless first heartbeat.
	select {
	case result := <-opened:
		m.handleOpenedStream(ctx, downID, res, sourceFormat, result)
		return
	default:
	}
	heartbeat := streamHeartbeat{format: sourceFormat, model: res.rec.UpstreamID, created: time.Now().Unix()}
	if err := m.bridge.StreamEmit(downID, heartbeat.payload()); err != nil {
		// Cancel the pending HTTP open, then drain its result before returning.
		// DoStream closes streams that arrive after cancellation.
		m.cancelPendingOpen(cancel, opened)
		_ = m.bridge.StreamCloseDownstream(downID, errclass.Redact(err.Error()))
		return
	}
	ticker := time.NewTicker(res.cfg.Streaming.KeepAliveInterval)
	defer ticker.Stop()
	for {
		select {
		case result := <-opened:
			m.handleOpenedStream(ctx, downID, res, sourceFormat, result)
			return
		case <-ctx.Done():
			m.cancelPendingOpen(cancel, opened)
			_ = m.bridge.StreamCloseDownstream(downID, "stream canceled")
			return
		case <-ticker.C:
			if err := m.bridge.StreamEmit(downID, heartbeat.payload()); err != nil {
				m.cancelPendingOpen(cancel, opened)
				_ = m.bridge.StreamCloseDownstream(downID, errclass.Redact(err.Error()))
				return
			}
		}
	}
}

func (m *Manager) handleOpenedStream(ctx context.Context, downID string, res *resolvedExecution, sourceFormat string, result streamOpenResult) {
	if result.failure != nil {
		_ = m.bridge.StreamCloseDownstream(downID, result.failure.Message)
		return
	}
	if result.status >= 400 {
		failure := m.streamStatusError(ctx, res, result)
		_ = m.bridge.StreamCloseDownstream(downID, failure.Message)
		return
	}
	if ctx.Err() != nil {
		_ = m.bridge.CloseStreamBounded(result.id)
		_ = m.bridge.StreamCloseDownstream(downID, "stream canceled")
		return
	}
	m.pumpStream(ctx, downID, result.id, res, sourceFormat)
}

func (m *Manager) cancelPendingOpen(cancel context.CancelFunc, opened <-chan streamOpenResult) {
	cancel()
	// DoStream handles late-created streams, while an already returned ID is
	// released here. Drain the one result before releasing the in-flight guard.
	result := <-opened
	if result.id != "" {
		_ = m.bridge.CloseStreamBounded(result.id)
	}
}

func (m *Manager) pumpStream(ctx context.Context, downID, upstreamID string, res *resolvedExecution, sourceFormat string) {
	readCtx, cancelRead := context.WithCancel(ctx)
	closeError := ""
	defer func() {
		cancelRead()
		_ = m.bridge.CloseStreamBounded(upstreamID)
		_ = m.bridge.StreamCloseDownstream(downID, closeError)
	}()

	reads := make(chan streamReadResult)
	m.streamTask(func() {
		defer close(reads)
		for {
			payload, message, closed, err := m.bridge.StreamReadContext(readCtx, upstreamID)
			select {
			case reads <- streamReadResult{payload, message, closed, err}:
			case <-readCtx.Done():
				return
			}
			if closed || message != "" || err != nil {
				return
			}
		}
	})
	idle := time.NewTimer(res.cfg.Streaming.IdleTimeout)
	defer idle.Stop()
	var keepalive <-chan time.Time
	if interval := res.cfg.Streaming.KeepAliveInterval; interval > 0 {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		keepalive = ticker.C
	}
	heartbeat := streamHeartbeat{format: sourceFormat, model: res.rec.UpstreamID, created: time.Now().Unix()}
	conv := newStreamConverter(res.rec.Protocol, sourceFormat, &res.tools)
	var total int64
	var pendingRead *streamReadResult
	// processRead is THE shared read handler for both the reads select arm
	// and a result rescued by the idle branch's non-blocking re-check, so
	// the two paths cannot diverge. It reports false when the loop must
	// stop; closeError is set on those paths.
	processRead := func(result streamReadResult) bool {
		if result.err != nil {
			closeError = errclass.Redact(result.err.Error())
			return false
		}
		if result.message != "" {
			closeError = errclass.Redact(result.message)
			return false
		}
		if len(result.payload) > 0 {
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			idle.Reset(res.cfg.Streaming.IdleTimeout)
		}
		total += int64(len(result.payload))
		if total > res.cfg.MaxResponseBytes {
			closeError = "stream exceeded max-response-bytes"
			return false
		}
		events, done, conversionError := conv.Feed(result.payload)
		if conversionError != nil {
			closeError = errclass.Redact(conversionError.Message)
			return false
		}
		heartbeat.observe(events)
		if err := m.emitAll(downID, events); err != nil {
			closeError = errclass.Redact(err.Error())
			return false
		}
		if done || result.closed {
			if !done && result.closed {
				if flusher, ok := conv.(interface{ Flush() [][]byte }); ok {
					if err := m.emitAll(downID, flusher.Flush()); err != nil {
						closeError = errclass.Redact(err.Error())
					}
				}
			}
			return false
		}
		return true
	}
	// pendingRead holds a result snatched from the idle branch's
	// non-blocking re-check; it is consumed before the next select.
	for {
		// Consume a result rescued from the idle branch before selecting
		// again, so it is neither stalled nor re-observed through reads.
		if pendingRead != nil {
			result := *pendingRead
			pendingRead = nil
			if !processRead(result) {
				return
			}
		}
		select {
		case <-ctx.Done():
			closeError = "stream canceled"
			if ctx.Err() == context.DeadlineExceeded {
				closeError = "stream exceeded streaming.max-duration"
			}
			return
		case <-idle.C:
			// F1: idle.C and reads can become ready in the same select
			// iteration; Go's uniform choice may otherwise kill a stream
			// whose data has already arrived. One non-blocking re-check
			// keeps "every upstream byte renews idle" true at the boundary.
			select {
			case result, ok := <-reads:
				if ok {
					pendingRead = &result
					continue
				}
			default:
			}
			closeError = "stream idle timeout"
			return
		case <-keepalive:
			if err := m.bridge.StreamEmit(downID, heartbeat.payload()); err != nil {
				closeError = errclass.Redact(err.Error())
				return
			}
		case result, ok := <-reads:
			if !ok {
				closeError = "upstream stream read ended unexpectedly"
				return
			}
			if ctx.Err() != nil {
				closeError = "stream canceled"
				return
			}
			if !processRead(result) {
				return
			}
		}
	}
}
