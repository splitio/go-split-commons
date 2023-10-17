package util

import (
	"testing"

	"github.com/splitio/go-toolkit/v5/logging"
)

func TestFlagSetCleanup(t *testing.T) {

	logger := logging.NewLogger(&logging.LoggerOptions{})

	validator := NewFlagSetValidator(logger)
	sets := []string{"Set1", " set3 ", "set_2", "set+4", "set-5", "set4", " set1"}

	validSets, invalidSets := validator.Cleanup(sets)

	if len(validSets) != 4 {
		t.Error("Valid sets size should be 5, but was", len(validSets))
	}
	if invalidSets != 2 {
		t.Error("Invalid sets should be 1, but was", invalidSets)
	}
}

func TestFlagSetAreValid(t *testing.T) {

	logger := logging.NewLogger(&logging.LoggerOptions{})

	validator := NewFlagSetValidator(logger)
	sets := []string{"Set1", " set3 ", "set_2", "set+4", "set-5", "set4", " set1"}

	cleanSets, valid := validator.AreValid(sets)

	if len(cleanSets) != 4 {
		t.Error("Valid sets size should be 5, but was", len(cleanSets))
	}
	if !valid {
		t.Error("Should be true, but was", valid)
	}
}
