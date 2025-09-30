package specs

import "testing"

func TestMatch(t *testing.T) {
	specVersion := "1.0"
	if Match(specVersion) == nil {
		t.Error("Expected 1.0")
	}

	specVersion = "1.1"
	if Match(specVersion) == nil {
		t.Error("Expected 1.1")
	}

	specVersion = "1.2"
	if Match(specVersion) == nil {
		t.Error("Expected 1.2")
	}

	specVersion = "1.3"
	if Match(specVersion) == nil {
		t.Error("Expected 1.3")
	}

	specVersion = "1.4"
	if Match(specVersion) != nil {
		t.Error("Expected nil")
	}
}
