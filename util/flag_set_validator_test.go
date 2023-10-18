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
	if validSets[0] != "set1" {
		t.Error("The first element should be set1, but was", validSets[0])
	}
	if validSets[1] != "set3" {
		t.Error("The first element should be set1, but was", validSets[1])
	}
	if validSets[2] != "set4" {
		t.Error("The first element should be set1, but was", validSets[2])
	}
	if validSets[3] != "set_2" {
		t.Error("The first element should be set1, but was", validSets[3])
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

func TestFlagSetValid(t *testing.T) {

	logger := logging.NewLogger(&logging.LoggerOptions{})

	validator := NewFlagSetValidator(logger)

	cleanSet, valid := validator.isFlagSetValid("set1")

	if cleanSet != "set1" {
		t.Error("Clean set should be set1, but was", cleanSet)
	}
	if !valid {
		t.Error("Should be true, but was", valid)
	}

	cleanSet, valid = validator.isFlagSetValid("Set1")

	if cleanSet != "set1" {
		t.Error("Clean set should be set1, but was", cleanSet)
	}
	if !valid {
		t.Error("Should be true, but was", valid)
	}

	cleanSet, valid = validator.isFlagSetValid(" set1")

	if cleanSet != "set1" {
		t.Error("Clean set should be set1, but was", cleanSet)
	}
	if !valid {
		t.Error("Should be true, but was", valid)
	}

	cleanSet, valid = validator.isFlagSetValid("set-1")

	if cleanSet != "" {
		t.Error("Clean set should be empty, but was", cleanSet)
	}
	if valid {
		t.Error("Should be true, but was", valid)
	}

	cleanSet, valid = validator.isFlagSetValid("set_1")

	if cleanSet != "set_1" {
		t.Error("Clean set should be set_1, but was", cleanSet)
	}
	if !valid {
		t.Error("Should be true, but was", valid)
	}
}
