package flagsets

import (
	"testing"
)

func TestFlagSetCleanup(t *testing.T) {
	sets := []string{"Set1", " set3 ", "set_2", "set+4", "set-5", "set4", " set1"}

	sanitizedSets, warnings := SanitizeMany(sets)

	if len(sanitizedSets) != 4 {
		t.Error("Valid sets size should be 5, but was", len(sanitizedSets))
	}
	if sanitizedSets[0] != "set1" {
		t.Error("The first element should be set1, but was", sanitizedSets[0])
	}
	if sanitizedSets[1] != "set3" {
		t.Error("The first element should be set1, but was", sanitizedSets[1])
	}
	if sanitizedSets[2] != "set4" {
		t.Error("The first element should be set1, but was", sanitizedSets[2])
	}
	if sanitizedSets[3] != "set_2" {
		t.Error("The first element should be set1, but was", sanitizedSets[3])
	}
	if len(warnings) != 5 {
		t.Error("Warning size should be 5, but was", len(warnings))
	}
}

func TestFlagSetValid(t *testing.T) {
	sanitizedSet, warnings := Sanitize("set1")

	if *sanitizedSet != "set1" {
		t.Error("Clean set should be set1, but was", sanitizedSet)
	}
	if len(warnings) != 0 {
		t.Error("Warning size should be 0, but was", len(warnings))
	}

	sanitizedSet, warnings = Sanitize("Set1")

	if *sanitizedSet != "set1" {
		t.Error("Clean set should be set1, but was", sanitizedSet)
	}
	if len(warnings) != 1 {
		t.Error("Warning size should be 1, but was", len(warnings))
	}

	sanitizedSet, warnings = Sanitize(" set1")

	if *sanitizedSet != "set1" {
		t.Error("Clean set should be set1, but was", sanitizedSet)
	}
	if len(warnings) != 1 {
		t.Error("Warning size should be 1, but was", len(warnings))
	}

	sanitizedSet, warnings = Sanitize("set-1")

	if sanitizedSet != nil {
		t.Error("Clean set should be empty, but was", sanitizedSet)
	}
	if len(warnings) != 1 {
		t.Error("Warning size should be 1, but was", len(warnings))
	}

	sanitizedSet, warnings = Sanitize("set_1")

	if *sanitizedSet != "set_1" {
		t.Error("Clean set should be set_1, but was", sanitizedSet)
	}
	if len(warnings) != 0 {
		t.Error("Warning size should be 0, but was", len(warnings))
	}
}
