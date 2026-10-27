package plugin

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

func TestCircuitBreaker_CrossModelShortCircuitIntegration(t *testing.T) {
	upstreamCalls := 0
	lastCalledModel := ""

	const multiModelCatalog = `{"data":[{"id":"glm-5.3"},{"id":"deepseek-v4.1"},{"id":"minimax-m3"}]}`

	f := &fakeCaller{}
	f.responder = wrapWithCatalog(multiModelCatalog, func(method string, payload []byte) ([]byte, error) {
		if method == pluginabi.MethodHostHTTPDo {
			var req pluginapi.HTTPRequest
			_ = json.Unmarshal(payload, &req)
			if strings.HasSuffix(req.URL, "/usage") {
				return hostOK(pluginapi.HTTPResponse{
					StatusCode: http.StatusOK,
					Body:       []byte(`{"usage":{"weekly":{"percent":100}}}`),
				}), nil
			}

			upstreamCalls++
			// First call (opencode-go/glm-5.3) returns 429 Quota Exhausted with reset time
			if upstreamCalls == 1 {
				lastCalledModel = "opencode-go/glm-5.3"
				body := []byte(`{"error":{"message":"Go usage limit exceeded: weekly limit reached (resets on 2026-11-02 13:42:07 GMT). Please try again later."}}`)
				return hostOK(pluginapi.HTTPResponse{
					StatusCode: http.StatusTooManyRequests,
					Body:       body,
				}), nil
			}

			// If subsequent calls reach here, record it
			lastCalledModel = "unexpected"
			return hostOK(pluginapi.HTTPResponse{
				StatusCode: http.StatusOK,
				Body:       []byte(`{"choices":[{"message":{"content":"ok"}}]}`),
			}), nil
		}
		if method == pluginabi.MethodHostHTTPDoStream {
			upstreamCalls++
			return hostOK(hostStreamStartResp{
				StatusCode: http.StatusOK,
				StreamID:   "stream-1",
			}), nil
		}
		return hostOK(map[string]any{}), nil
	})

	m := NewManager(NewHostBridge(f.call))
	t.Cleanup(func() { _, _ = m.HandleCall("plugin.shutdown", nil) })

	if _, err := m.HandleCall("plugin.register", lifecycleRequestBody(testValidYAML)); err != nil {
		t.Fatalf("register: %v", err)
	}

	key := testKey

	// Step 1: Request model A (opencode-go/glm-5.3) -> Trips circuit breaker on 429
	req1 := executorRequest{
		ExecutorRequest: pluginapi.ExecutorRequest{
			Model:           "opencode-go/glm-5.3",
			SourceFormat:    "openai",
			Stream:          false,
			OriginalRequest: []byte(ccRequestBody),
			AuthProvider:    ProviderID,
			AuthAttributes:  map[string]string{"api_key": key},
		},
	}
	rawReq1, _ := json.Marshal(req1)
	resp1, err := m.handleExecute(rawReq1)
	if err != nil {
		t.Fatalf("handleExecute err: %v", err)
	}

	var env1 pluginabi.Envelope
	_ = json.Unmarshal(resp1, &env1)
	if env1.OK || env1.Error == nil || env1.Error.HTTPStatus != http.StatusTooManyRequests {
		t.Fatalf("expected 429 error on first call, got env: %+v", env1)
	}
	if upstreamCalls != 1 {
		t.Fatalf("expected exactly 1 upstream call, got %d", upstreamCalls)
	}

	// Step 2: Request model B (opencode-go/minimax-m3) using SAME API Key
	// Must be intercepted by Circuit Breaker in memory! 0 UPSTREAM CALLS!
	req2 := executorRequest{
		ExecutorRequest: pluginapi.ExecutorRequest{
			Model:           "opencode-go/minimax-m3",
			SourceFormat:    "openai",
			Stream:          false,
			OriginalRequest: []byte(ccRequestBody),
			AuthProvider:    ProviderID,
			AuthAttributes:  map[string]string{"api_key": key},
		},
	}
	rawReq2, _ := json.Marshal(req2)
	resp2, err := m.handleExecute(rawReq2)
	if err != nil {
		t.Fatalf("handleExecute 2 err: %v", err)
	}

	var env2 pluginabi.Envelope
	_ = json.Unmarshal(resp2, &env2)
	if env2.OK || env2.Error == nil || env2.Error.HTTPStatus != http.StatusTooManyRequests {
		t.Fatalf("expected short-circuited 429 on second model, got env: %+v", env2)
	}
	if !strings.Contains(env2.Error.Message, "circuit breaker active") {
		t.Fatalf("expected circuit breaker message, got %q", env2.Error.Message)
	}

	// The golden check: upstreamCalls MUST STILL BE 1! Absolutely NO penetration!
	if upstreamCalls != 1 {
		t.Fatalf("CIRCUIT BREAKER LEAKED! Expected exactly 1 upstream call, but saw %d calls! Last called model: %s",
			upstreamCalls, lastCalledModel)
	}

	// Step 3: Stream request on model C (opencode-go/minimax-m3) using SAME API Key
	// Must also be short-circuited!
	req3 := executorRequest{
		ExecutorRequest: pluginapi.ExecutorRequest{
			Model:           "opencode-go/minimax-m3",
			SourceFormat:    "openai",
			Stream:          true,
			OriginalRequest: []byte(ccRequestBody),
			AuthProvider:    ProviderID,
			AuthAttributes:  map[string]string{"api_key": key},
		},
		StreamID: "stream-test-1",
	}
	rawReq3, _ := json.Marshal(req3)
	resp3, err := m.handleExecuteStream(rawReq3)
	if err != nil {
		t.Fatalf("handleExecuteStream err: %v", err)
	}

	var env3 pluginabi.Envelope
	_ = json.Unmarshal(resp3, &env3)
	if env3.OK || env3.Error == nil || env3.Error.HTTPStatus != http.StatusTooManyRequests {
		t.Fatalf("expected short-circuited 429 on stream model, got env: %+v", env3)
	}

	// Still exactly 1 upstream call!
	if upstreamCalls != 1 {
		t.Fatalf("CIRCUIT BREAKER STREAM LEAKED! Expected 1 upstream call, got %d", upstreamCalls)
	}
}
