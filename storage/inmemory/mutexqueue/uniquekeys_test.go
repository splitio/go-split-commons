package mutexqueue

import (
	"testing"

	"github.com/splitio/go-split-commons/v7/dtos"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestMQUniqueKeysStorage(t *testing.T) {
	isFull := make(chan string, 1)
	logger := logging.NewLogger(&logging.LoggerOptions{})
	storage := NewMQUniqueKeysStorage(5, isFull, logger)

	// Push some items into the queue
	storage.Push("feature-1", "key-1")
	storage.Push("feature-1", "key-2")
	storage.Push("feature-2", "key-3")
	storage.Push("feature-2", "key-4")
	storage.Push("feature-3", "key-5")

	// Test PopN with n less than the queue size
	result := storage.PopN(3)
	if len(result.Keys) != 2 {
		t.Errorf("Expected 2 feature groups, got %d", len(result.Keys))
	}

	// Validate feature-1 keys
	feature1Keys := findFeatureKeys(result.Keys, "feature-1")
	if len(feature1Keys) != 2 || !contains(feature1Keys, "key-1") || !contains(feature1Keys, "key-2") {
		t.Errorf("Unexpected keys for feature-1: %v", feature1Keys)
	}

	// Validate feature-2 keys
	feature2Keys := findFeatureKeys(result.Keys, "feature-2")
	if len(feature2Keys) != 1 || !contains(feature2Keys, "key-3") {
		t.Errorf("Unexpected keys for feature-2: %v", feature2Keys)
	}

	// Test PopN with n greater than the remaining queue size
	result = storage.PopN(5)
	if len(result.Keys) != 2 {
		t.Errorf("Expected 2 feature groups, got %d", len(result.Keys))
	}

	// Validate feature-2 keys
	feature2Keys = findFeatureKeys(result.Keys, "feature-2")
	if len(feature2Keys) != 1 || !contains(feature2Keys, "key-4") {
		t.Errorf("Unexpected keys for feature-2: %v", feature2Keys)
	}

	// Validate feature-3 keys
	feature3Keys := findFeatureKeys(result.Keys, "feature-3")
	if len(feature3Keys) != 1 || !contains(feature3Keys, "key-5") {
		t.Errorf("Unexpected keys for feature-3: %v", feature3Keys)
	}

	// Test PopN with an empty queue
	result = storage.PopN(1)
	if len(result.Keys) != 0 {
		t.Errorf("Expected 0 feature groups, got %d", len(result.Keys))
	}
}

// Helper function to find keys for a specific feature
func findFeatureKeys(keys []dtos.Key, feature string) []interface{} {
	for _, key := range keys {
		if key.Feature == feature {
			return key.Keys
		}
	}
	return nil
}

// Helper function to check if a slice contains a specific value
func contains(slice []interface{}, value string) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}
