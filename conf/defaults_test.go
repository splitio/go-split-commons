package conf

import "testing"

func TestGetDefaultAdvancedConfigStreamingForceHTTP1(t *testing.T) {
	// StreamingForceHTTP1 defaults to false: streaming uses HTTP/2 by default, and
	// forcing HTTP/1.1 is an explicit opt-in escape hatch.
	cfg := GetDefaultAdvancedConfig()
	if cfg.StreamingForceHTTP1 {
		t.Error("StreamingForceHTTP1 should default to false (HTTP/2 by default)")
	}
}
