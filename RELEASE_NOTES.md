## v0.2.5

### Bug Fixes & Enhancements

- **Quota & Error Classification**: Aligned upstream HTTP 429 quota classification with OpenCode-Go usage limit taxonomy and CLIProxyAPI SDK specifications. Error messages matching `"Go usage limit exceeded"`, `"usage_limit_reached"`, `"usagelimit"`, `"balance"`, and `"budget"` are now accurately classified as `ClassQuota` (`quota_exhaustion`).

## v0.2.4

### What's Changed

### Bug Fixes & Enhancements

- **SSE Stream Truncation & Framing**: Added `Flush()` support to `StreamScanner` to flush any unbuffered trailing chunk data upon EOF, preventing truncated tool call arguments or content deltas.
- **CGO Memory Safety**: Fixed memory leak where `callHost` allocated 1 byte for empty responses without freeing via `C.free()`. Ensured host pointer is safely reset to nil on shutdown.
- **Provider Protocol Alignment**:
  - Implemented `MethodPluginQuiesce` (`plugin.quiesce`) for smooth runtime draining and lifecycle shutdown.
  - Relaxed provider ID validation in `AuthRecords` to support all configured provider instances.
  - Hardened legacy auth discovery against missing or permission-denied directories.
  - Enhanced model discovery metadata with `Name`, `InputTokenLimit`, and `OutputTokenLimit`.
  - Enforced `stream: true` in outbound stream payloads and corrected misleading error messages.
  - Added dynamic quota window parsing support for custom reset duration units.
- **UTF-8 Safety**: Hardened string boundary slicing to prevent corrupting multi-byte UTF-8 runes in partial response chunks.
- **Test Suite Optimization**: Resolved blocking catalog 15-second drain lag in integration tests, reducing test suite execution time significantly.

- **Streaming Lifecycle & Thinking Resilience**:
  - Dedicated `streaming` configuration (`header-timeout`, `idle-timeout`, `max-duration`, `keepalive-interval`) decoupling long-running thinking streams from non-streaming `request-timeout`.
  - Cross-protocol `reasoning_content` synthesis for Chat Completions models (`glm-5.3-flash`, `deepseek-v4.1-flash`, `kimi-k2.5`) into Anthropic `thinking` blocks and OpenAI Responses `reasoning` summary events/items.
  - Capability-aware `reasoning.effort` clamping (`ClampEffort`) so out-of-range canonical levels (such as `"max"`) map cleanly to the target model's supported set.
- **Responses Tool Image Support**: Allow `function_call_output` to carry multimodal images (`input_image` / `image_url`) when translating from OpenAI Responses to Chat Completions and Anthropic Messages, resolving HTTP 400 errors during tool image inspections in agents like OMP.

## Upgrade Notes

- Replace the old plugin binary with the new release binary.
- Restart CLIProxyAPI after replacing the plugin.

**Full Changelog**: https://github.com/massiveits/opencode-go-cliproxyapi/compare/v0.2.3...v0.2.4
