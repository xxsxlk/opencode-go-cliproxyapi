package plugin

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

func TestGLMFlashThinkingContinuesBeyondRequestTimeout(t *testing.T) {
	const model = "glm-5.3-flash"
	f := &fakeCaller{}
	m := NewManager(NewHostBridge(f.call))
	t.Cleanup(func() { _, _ = m.HandleCall(pluginabi.MethodPluginShutdown, nil) })
	const thinking = "data: {\"id\":\"msg-glm\",\"model\":\"glm-5.3-flash\",\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"thinking\"}}]}\n\n"
	const answer = "data: {\"id\":\"msg-glm\",\"model\":\"glm-5.3-flash\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ANSWER\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"
	reads := 0
	f.responder = wrapWithCatalog(`{"data":[{"id":"glm-5.3-flash"}]}`, func(method string, _ []byte) ([]byte, error) {
		switch method {
		case pluginabi.MethodHostHTTPDoStream:
			return hostOK(hostStreamStartResp{StatusCode: 200, StreamID: "up-glm-flash"}), nil
		case pluginabi.MethodHostHTTPStreamRead:
			time.Sleep(8 * time.Millisecond)
			reads++
			if reads <= 40 {
				return hostOK(hostStreamReadResp{Payload: []byte(thinking)}), nil
			}
			return hostOK(hostStreamReadResp{Payload: []byte(answer), Done: true}), nil
		}
		return hostOK(map[string]any{}), nil
	})
	cfg := testValidYAML + "request-timeout: 50ms\nstreaming:\n  idle-timeout: 100ms\n  keepalive-interval: 25ms\n"
	if raw, err := m.HandleCall(pluginabi.MethodPluginRegister, lifecycleRequestBody(cfg)); err != nil || !decodeEnv(t, raw).OK {
		t.Fatal("registration failed")
	}
	start := time.Now()
	raw, err := m.HandleCall(pluginabi.MethodExecutorExecuteStream,
		execStreamReqBody("opencode-go/"+model, "claude", []byte(ccRequestBody), "down-glm"))
	if err != nil || !decodeEnv(t, raw).OK {
		t.Fatal("stream start failed")
	}
	if !m.bridge.WaitForInFlight(3 * time.Second) {
		t.Fatal("thinking stream was not completed")
	}
	if elapsed := time.Since(start); elapsed < 200*time.Millisecond {
		t.Fatalf("stream did not outlive old request-timeout: %v", elapsed)
	}
	var output strings.Builder
	for _, call := range f.callsOf(pluginabi.MethodHostStreamEmit) {
		var chunk struct {
			Payload []byte `json:"payload"`
		}
		if err := json.Unmarshal(call.payload, &chunk); err != nil {
			t.Fatal(err)
		}
		output.Write(chunk.Payload)
	}
	if !strings.Contains(output.String(), "ANSWER") || !strings.Contains(output.String(), ": keep-alive\n\n") ||
		!strings.Contains(output.String(), "event: message_stop") {
		t.Fatalf("thinking, keepalive, or answer lost: %s", output.String())
	}
	closes := f.callsOf(pluginabi.MethodHostStreamClose)
	if len(closes) != 1 || strings.Contains(string(closes[0].payload), `"error"`) {
		t.Fatalf("active thinking must end cleanly: %v", closes)
	}
}

func TestSlowHeaderSendsHeartbeatThenReportsLateFailure(t *testing.T) {
	f := &fakeCaller{}
	m := NewManager(NewHostBridge(f.call))
	t.Cleanup(func() { _, _ = m.HandleCall(pluginabi.MethodPluginShutdown, nil) })
	f.responder = wrapWithCatalog(testCatalogJSON, func(method string, _ []byte) ([]byte, error) {
		switch method {
		case pluginabi.MethodHostHTTPDoStream:
			time.Sleep(85 * time.Millisecond)
			return hostOK(hostStreamStartResp{StatusCode: 503, StreamID: "up-slow-fail"}), nil
		case pluginabi.MethodHostHTTPStreamRead:
			return hostOK(hostStreamReadResp{Payload: []byte(`{"error":{"message":"upstream unavailable"}}`), Done: true}), nil
		}
		return hostOK(map[string]any{}), nil
	})
	cfg := testValidYAML + "streaming:\n  header-timeout: 200ms\n  keepalive-interval: 15ms\n"
	if raw, err := m.HandleCall(pluginabi.MethodPluginRegister, lifecycleRequestBody(cfg)); err != nil || !decodeEnv(t, raw).OK {
		t.Fatal("registration failed")
	}
	start := time.Now()
	raw, err := m.HandleCall(pluginabi.MethodExecutorExecuteStream,
		execStreamReqBody("opencode-go/glm-5.3", "claude", []byte(ccRequestBody), "down-slow"))
	if err != nil || !decodeEnv(t, raw).OK || time.Since(start) >= 500*time.Millisecond {
		t.Fatalf("waiting for slow upstream blocked bootstrap: %v", err)
	}
	if !m.bridge.WaitForInFlight(3 * time.Second) {
		t.Fatal("late failure did not close the stream")
	}
	emits := f.callsOf(pluginabi.MethodHostStreamEmit)
	var first struct {
		Payload []byte `json:"payload"`
	}
	if len(emits) == 0 {
		t.Fatal("missing pre-header heartbeat")
	}
	if err := json.Unmarshal(emits[0].payload, &first); err != nil || string(first.Payload) != ": keep-alive\n\n" {
		t.Fatalf("invalid pre-header heartbeat: %q, %v", first.Payload, err)
	}
	closes := f.callsOf(pluginabi.MethodHostStreamClose)
	if len(closes) != 1 || !strings.Contains(string(closes[0].payload), "upstream unavailable") {
		t.Fatalf("late upstream error was not reported: %v", closes)
	}
	if len(f.callsOf(pluginabi.MethodHostHTTPStreamClose)) != 1 {
		t.Fatal("late failure left an upstream stream open")
	}
}

func TestKeepaliveCannotExtendStreamIdleLimit(t *testing.T) {
	f := &fakeCaller{}
	m := NewManager(NewHostBridge(f.call))
	t.Cleanup(func() { _, _ = m.HandleCall(pluginabi.MethodPluginShutdown, nil) })
	blocked := make(chan struct{})
	var once sync.Once
	f.responder = wrapWithCatalog(testCatalogJSON, func(method string, _ []byte) ([]byte, error) {
		switch method {
		case pluginabi.MethodHostHTTPDoStream:
			return hostOK(hostStreamStartResp{StatusCode: http.StatusOK, StreamID: "up-no-data"}), nil
		case pluginabi.MethodHostHTTPStreamRead:
			<-blocked
			return hostOK(hostStreamReadResp{Done: true}), nil
		case pluginabi.MethodHostHTTPStreamClose:
			once.Do(func() { close(blocked) })
		}
		return hostOK(map[string]any{}), nil
	})
	cfg := testValidYAML + "streaming:\n  idle-timeout: 85ms\n  keepalive-interval: 15ms\n"
	if raw, err := m.HandleCall(pluginabi.MethodPluginRegister, lifecycleRequestBody(cfg)); err != nil || !decodeEnv(t, raw).OK {
		t.Fatal("registration failed")
	}
	raw, err := m.HandleCall(pluginabi.MethodExecutorExecuteStream,
		execStreamReqBody("opencode-go/glm-5.3", "openai", []byte(ccRequestBody), "down-idle"))
	if err != nil || !decodeEnv(t, raw).OK {
		t.Fatal("stream start failed")
	}
	if !m.bridge.WaitForInFlight(3 * time.Second) {
		t.Fatal("idle stream not released")
	}
	if len(f.callsOf(pluginabi.MethodHostStreamEmit)) == 0 ||
		!strings.Contains(string(f.callsOf(pluginabi.MethodHostStreamClose)[0].payload), "stream idle timeout") {
		t.Fatal("heartbeats incorrectly extended the idle deadline")
	}
}

func TestStreamHeartbeatPayloads(t *testing.T) {
	for _, format := range []string{"openai", "claude", "openai-response"} {
		h := streamHeartbeat{format: format, model: "glm-5.3-flash", created: time.Now().Unix()}
		raw := h.payload()
		switch format {
		case "openai":
			var event struct {
				ID      string `json:"id"`
				Choices []struct {
					Delta        map[string]any `json:"delta"`
					FinishReason *string        `json:"finish_reason"`
				} `json:"choices"`
			}
			if json.Unmarshal(raw, &event) != nil || event.ID != "" || len(event.Choices) != 1 || len(event.Choices[0].Delta) != 0 || event.Choices[0].FinishReason != nil {
				t.Fatalf("invalid no-content Chat Completions keepalive: %s", raw)
			}
		case "claude":
			if string(raw) != ": keep-alive\n\n" {
				t.Fatalf("invalid Claude SSE comment: %q", raw)
			}
		case "openai-response":
			if string(raw) != "event: ping\ndata: {}\n\n" {
				t.Fatalf("invalid Responses JSON ping: %q", raw)
			}
		}
	}
}

func TestAwaitStreamOpenPrioritizesReadyStatusOverHeartbeat(t *testing.T) {
	for i := 0; i < 1000; i++ {
		opened := make(chan streamOpenResult, 1)
		grace := make(chan time.Time, 1)
		opened <- streamOpenResult{status: 429, id: "up-rejected"}
		grace <- time.Now()
		result, ready := awaitStreamOpen(opened, grace)
		if !ready || result.status != 429 {
			t.Fatal("a known upstream rejection lost to the grace timer")
		}
	}
}

func TestFastUpstreamStatusWithStalledBodyRemainsRetryable(t *testing.T) {
	f := &fakeCaller{}
	m := NewManager(NewHostBridge(f.call))
	t.Cleanup(func() { _, _ = m.HandleCall(pluginabi.MethodPluginShutdown, nil) })
	blocked := make(chan struct{})
	var once sync.Once
	f.responder = wrapWithCatalog(testCatalogJSON, func(method string, _ []byte) ([]byte, error) {
		switch method {
		case pluginabi.MethodHostHTTPDoStream:
			return hostOK(hostStreamStartResp{StatusCode: 429, StreamID: "up-rejected"}), nil
		case pluginabi.MethodHostHTTPStreamRead:
			<-blocked
			return hostOK(hostStreamReadResp{Done: true}), nil
		case pluginabi.MethodHostHTTPStreamClose:
			once.Do(func() { close(blocked) })
		}
		return hostOK(map[string]any{}), nil
	})
	cfg := testValidYAML + "request-timeout: 75ms\nstreaming:\n  keepalive-interval: 15ms\n"
	if raw, err := m.HandleCall(pluginabi.MethodPluginRegister, lifecycleRequestBody(cfg)); err != nil || !decodeEnv(t, raw).OK {
		t.Fatal("registration failed")
	}
	start := time.Now()
	raw, err := m.HandleCall(pluginabi.MethodExecutorExecuteStream,
		execStreamReqBody("opencode-go/glm-5.3", "openai", []byte(ccRequestBody), "down-rejected"))
	if err != nil {
		t.Fatal(err)
	}
	env := decodeEnv(t, raw)
	if env.OK || env.Error == nil || env.Error.HTTPStatus != 429 || !env.Error.Retryable {
		t.Fatalf("HTTP rejection lost retryable status: %+v", env.Error)
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("error body was not really stalled: %s", elapsed)
	}
	if len(f.callsOf(pluginabi.MethodHostStreamEmit)) != 0 || len(f.callsOf(pluginabi.MethodHostStreamClose)) != 0 {
		t.Fatal("HTTP failure sent success heartbeats before retry")
	}
}

func TestDisabledKeepalivePreservesPreHeaderFailure(t *testing.T) {
	f := &fakeCaller{}
	m := NewManager(NewHostBridge(f.call))
	t.Cleanup(func() { _, _ = m.HandleCall(pluginabi.MethodPluginShutdown, nil) })
	f.responder = wrapWithCatalog(testCatalogJSON, func(method string, _ []byte) ([]byte, error) {
		switch method {
		case pluginabi.MethodHostHTTPDoStream:
			time.Sleep(55 * time.Millisecond)
			return hostOK(hostStreamStartResp{StatusCode: 429, StreamID: "up-no-bootstrap"}), nil
		case pluginabi.MethodHostHTTPStreamRead:
			return hostOK(hostStreamReadResp{Payload: []byte(`{"error":{"message":"rate limited"}}`), Done: true}), nil
		}
		return hostOK(map[string]any{}), nil
	})
	cfg := testValidYAML + "streaming:\n  header-timeout: 150ms\n  keepalive-interval: 0s\n"
	if raw, err := m.HandleCall(pluginabi.MethodPluginRegister, lifecycleRequestBody(cfg)); err != nil || !decodeEnv(t, raw).OK {
		t.Fatal("registration failed")
	}
	start := time.Now()
	raw, err := m.HandleCall(pluginabi.MethodExecutorExecuteStream,
		execStreamReqBody("opencode-go/glm-5.3", "openai", []byte(ccRequestBody), "down-no-bootstrap"))
	if err != nil {
		t.Fatal(err)
	}
	env := decodeEnv(t, raw)
	if env.OK || env.Error == nil || env.Error.HTTPStatus != 429 || time.Since(start) < 40*time.Millisecond ||
		len(f.callsOf(pluginabi.MethodHostStreamEmit)) != 0 {
		t.Fatalf("zero keepalive must not commit SSE while waiting for headers: %+v", env.Error)
	}
}

func TestStreamingOptionalMaxDurationStopsActiveInput(t *testing.T) {
	f := &fakeCaller{}
	m := NewManager(NewHostBridge(f.call))
	t.Cleanup(func() { _, _ = m.HandleCall(pluginabi.MethodPluginShutdown, nil) })
	blocked := make(chan struct{})
	var once sync.Once
	f.responder = wrapWithCatalog(testCatalogJSON, func(method string, _ []byte) ([]byte, error) {
		switch method {
		case pluginabi.MethodHostHTTPDoStream:
			return hostOK(hostStreamStartResp{StatusCode: 200, StreamID: "up-max"}), nil
		case pluginabi.MethodHostHTTPStreamRead:
			select {
			case <-blocked:
				return hostOK(hostStreamReadResp{Done: true}), nil
			case <-time.After(10 * time.Millisecond):
				return hostOK(hostStreamReadResp{Payload: []byte(": ping\n\n")}), nil
			}
		case pluginabi.MethodHostHTTPStreamClose:
			once.Do(func() { close(blocked) })
		}
		return hostOK(map[string]any{}), nil
	})
	cfg := testValidYAML + "streaming:\n  max-duration: 150ms\n  idle-timeout: 80ms\n  keepalive-interval: 15ms\n"
	if raw, err := m.HandleCall(pluginabi.MethodPluginRegister, lifecycleRequestBody(cfg)); err != nil || !decodeEnv(t, raw).OK {
		t.Fatal("registration failed")
	}
	raw, err := m.HandleCall(pluginabi.MethodExecutorExecuteStream,
		execStreamReqBody("opencode-go/glm-5.3", "claude", []byte(ccRequestBody), "down-max"))
	if err != nil || !decodeEnv(t, raw).OK {
		t.Fatal("stream start failed")
	}
	if !m.bridge.WaitForInFlight(3 * time.Second) {
		t.Fatal("max-duration did not stop active stream")
	}
	closes := f.callsOf(pluginabi.MethodHostStreamClose)
	if len(closes) != 1 || !strings.Contains(string(closes[0].payload), "stream exceeded streaming.max-duration") {
		t.Fatalf("active stream was not limited by max-duration: %v", closes)
	}
}

func TestShutdownCancelsActiveThinkingAndDrainsCallbacks(t *testing.T) {
	f := &fakeCaller{}
	m := NewManager(NewHostBridge(f.call))
	blocked := make(chan struct{})
	reading := make(chan struct{})
	var once sync.Once
	f.responder = wrapWithCatalog(testCatalogJSON, func(method string, _ []byte) ([]byte, error) {
		switch method {
		case pluginabi.MethodHostHTTPDoStream:
			return hostOK(hostStreamStartResp{StatusCode: 200, StreamID: "up-shutdown"}), nil
		case pluginabi.MethodHostHTTPStreamRead:
			once.Do(func() { close(reading) })
			<-blocked
			return hostOK(hostStreamReadResp{Done: true}), nil
		case pluginabi.MethodHostHTTPStreamClose:
			close(blocked)
		}
		return hostOK(map[string]any{}), nil
	})
	if raw, err := m.HandleCall(pluginabi.MethodPluginRegister, lifecycleRequestBody(testValidYAML)); err != nil || !decodeEnv(t, raw).OK {
		t.Fatal("registration failed")
	}
	raw, err := m.HandleCall(pluginabi.MethodExecutorExecuteStream,
		execStreamReqBody("opencode-go/glm-5.3", "openai", []byte(ccRequestBody), "down-shutdown"))
	if err != nil || !decodeEnv(t, raw).OK {
		t.Fatal("stream start failed")
	}
	select {
	case <-reading:
	case <-time.After(time.Second):
		t.Fatal("read did not start")
	}
	stopped := make(chan struct{})
	go func() { _, _ = m.HandleCall(pluginabi.MethodPluginShutdown, nil); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not cancel the live stream")
	}
	if len(f.callsOf(pluginabi.MethodHostHTTPStreamClose)) != 1 ||
		len(f.callsOf(pluginabi.MethodHostStreamClose)) != 1 || !m.bridge.WaitForInFlight(time.Second) {
		t.Fatal("shutdown left an active host callback")
	}
}

func TestLateOpenCloseTimeoutDoesNotBlockCancellation(t *testing.T) {
	oldGrace, oldEmit := streamTimeoutGrace, emitTimeout
	streamTimeoutGrace, emitTimeout = 10*time.Millisecond, 20*time.Millisecond
	defer func() { streamTimeoutGrace, emitTimeout = oldGrace, oldEmit }()
	f := &fakeCaller{}
	m := NewManager(NewHostBridge(f.call))
	closed := make(chan struct{})
	var closeOnce sync.Once
	f.responder = wrapWithCatalog(testCatalogJSON, func(method string, _ []byte) ([]byte, error) {
		switch method {
		case pluginabi.MethodHostHTTPDoStream:
			time.Sleep(65 * time.Millisecond)
			return hostOK(hostStreamStartResp{StatusCode: 200, StreamID: "up-late"}), nil
		case pluginabi.MethodHostHTTPStreamClose:
			time.Sleep(130 * time.Millisecond)
			closeOnce.Do(func() { close(closed) })
		}
		return hostOK(map[string]any{}), nil
	})
	cfg := testValidYAML + "streaming:\n  header-timeout: 20ms\n  keepalive-interval: 5ms\n"
	if raw, err := m.HandleCall(pluginabi.MethodPluginRegister, lifecycleRequestBody(cfg)); err != nil || !decodeEnv(t, raw).OK {
		t.Fatal("registration failed")
	}
	raw, err := m.HandleCall(pluginabi.MethodExecutorExecuteStream,
		execStreamReqBody("opencode-go/glm-5.3", "claude", []byte(ccRequestBody), "down-late"))
	if err != nil || !decodeEnv(t, raw).OK {
		t.Fatal("slow open did not start")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("late upstream close was never attempted")
	}
	if !m.bridge.WaitForInFlight(time.Second) {
		t.Fatal("late cleanup outlived callbacks")
	}
	if len(f.callsOf(pluginabi.MethodHostHTTPStreamClose)) != 1 {
		t.Fatal("late stream was not released")
	}
	_, _ = m.HandleCall(pluginabi.MethodPluginShutdown, nil)
}

// TestIdleTimeoutDoesNotKillArrivedData hammers the boundary where idle.C
// and reads become ready in the same select iteration: the rescued result
// must be processed as real content, never discarded with an idle kill.
func TestIdleTimeoutDoesNotKillArrivedData(t *testing.T) {
	oldEmit := emitTimeout
	emitTimeout = 60 * time.Millisecond
	defer func() { emitTimeout = oldEmit }()

	const frame = `data: {"id":"c1","choices":[{"index":0,"delta":{"content":"x"}}]}` + "\n\n"
	const answer = `data: {"id":"c1","choices":[{"index":0,"delta":{"content":"ANSWER"}}],"usage":{"prompt_tokens":1,"completion_tokens":2}}` + "\n\n" + "data: [DONE]\n\n"
	iteration := 0
	reads := 0
	f := &fakeCaller{}
	m := NewManager(NewHostBridge(f.call))
	t.Cleanup(func() { _, _ = m.HandleCall(pluginabi.MethodPluginShutdown, nil) })
	f.responder = wrapWithCatalog(testCatalogJSON, func(method string, _ []byte) ([]byte, error) {
		switch method {
		case pluginabi.MethodHostHTTPDoStream:
			return hostOK(hostStreamStartResp{StatusCode: 200, StreamID: "up-idle-race"}), nil
		case pluginabi.MethodHostHTTPStreamRead:
			reads++
			if reads == 1 {
				return hostOK(hostStreamReadResp{Payload: []byte(frame)}), nil
			}
			return hostOK(hostStreamReadResp{Payload: []byte(answer), Done: true}), nil
		}
		return hostOK(map[string]any{}), nil
	})
	cfg := testValidYAML + "streaming:\n  idle-timeout: 5ms\n  keepalive-interval: 0s\n"
	if raw, err := m.HandleCall(pluginabi.MethodPluginRegister, lifecycleRequestBody(cfg)); err != nil || !decodeEnv(t, raw).OK {
		t.Fatal("registration failed")
	}
	for range 200 {
		iteration++
		raw, err := m.HandleCall(pluginabi.MethodExecutorExecuteStream,
			execStreamReqBody("opencode-go/glm-5.3", "openai", []byte(ccRequestBody), "down-idle-race"))
		if err != nil || !decodeEnv(t, raw).OK {
			t.Fatalf("iteration %d: stream start failed: %v %s", iteration, err, raw)
		}
		if !m.bridge.WaitForInFlight(3 * time.Second) {
			t.Fatalf("iteration %d: stream never finished", iteration)
		}
		var output strings.Builder
		for _, call := range f.callsOf(pluginabi.MethodHostStreamEmit) {
			var chunk struct {
				Payload []byte `json:"payload"`
			}
			if json.Unmarshal(call.payload, &chunk) == nil {
				output.Write(chunk.Payload)
			}
		}
		if !strings.Contains(output.String(), "ANSWER") {
			t.Fatalf("iteration %d: data that arrived at the idle boundary was killed: %s", iteration, output.String())
		}
	}
}

// TestConcurrentShutdownAndStreamStart hammers the registration window a
// request crosses between its parent Err()-check and its inFlight.Add: the
// RWMutex-serialized protocol must never let shutdown finish Wait while a
// started request is still uncounted (which would dlclose under its host
// callbacks).
func TestConcurrentShutdownAndStreamStart(t *testing.T) {
	oldDrain := shutdownDrainTimeout
	shutdownDrainTimeout = 300 * time.Millisecond
	defer func() { shutdownDrainTimeout = oldDrain }()

	started := make(chan struct{}, 64)
	release := make(chan struct{})
	var once sync.Once
	f := &fakeCaller{responder: func(method string, payload []byte) ([]byte, error) {
		switch method {
		case pluginabi.MethodHostLog, pluginabi.MethodHostAuthList, pluginabi.MethodHostAuthSave:
			return hostOK(map[string]any{}), nil
		case pluginabi.MethodHostHTTPDo:
			var wire map[string]any
			_ = json.Unmarshal(payload, &wire)
			if url, _ := wire["url"].(string); strings.HasSuffix(url, "/models") {
				return hostOK(pluginapi.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(testCatalogJSON)}), nil
			}
			return hostOK(pluginapi.HTTPResponse{StatusCode: http.StatusOK, Body: []byte(ccResponseBody)}), nil
		case pluginabi.MethodHostHTTPDoStream:
			return hostOK(hostStreamStartResp{StatusCode: 200, StreamID: "up-race"}), nil
		case pluginabi.MethodHostHTTPStreamRead:
			once.Do(func() { close(started) })
			<-release
			return hostOK(hostStreamReadResp{Done: true}), nil
		case pluginabi.MethodHostHTTPStreamClose, pluginabi.MethodHostStreamClose:
			once.Do(func() { close(release) })
		}
		return hostOK(map[string]any{}), nil
	}}
	m := NewManager(NewHostBridge(f.call))

	if raw, err := m.HandleCall(pluginabi.MethodPluginRegister, lifecycleRequestBody(testValidYAML)); err != nil || !decodeEnv(t, raw).OK {
		t.Fatal("registration failed")
	}

	const workers = 32
	var wg sync.WaitGroup
	stopped := make(chan struct{})
	for range workers {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopped:
					return
				default:
				}
				_, _ = m.HandleCall(pluginabi.MethodExecutorExecuteStream,
					execStreamReqBody("opencode-go/glm-5.3", "openai", []byte(ccRequestBody), "down-race"))
			}
		}()
		go func() {
			defer wg.Done()
			_, _ = m.HandleCall(pluginabi.MethodPluginShutdown, nil)
		}()
	}
	// Let the race churn briefly, then end it by unblocking reads.
	time.Sleep(50 * time.Millisecond)
	once.Do(func() { close(release) })
	close(stopped)
	wg.Wait()
	if !m.bridge.WaitForInFlight(3 * time.Second) {
		t.Fatal("shutdown left host callbacks in flight after the race window")
	}
}
