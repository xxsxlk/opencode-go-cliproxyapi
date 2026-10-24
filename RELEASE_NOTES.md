## What's Changed

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

## Upgrade Notes

- Replace the old plugin binary with the new release binary.
- Restart CLIProxyAPI after replacing the plugin.

**Full Changelog**: https://github.com/massiveits/opencode-go-cliproxyapi/compare/v0.2.2...v0.2.3
