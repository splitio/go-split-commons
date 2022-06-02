package filter

import (
	"testing"

	"github.com/splitio/go-split-commons/v4/storage/mocks"
)

func Test(t *testing.T) {
	featureName1 := "feature-name-test"
	featureName2 := "feature-name-second-test"
	key := "key-example-expected"

	adapter := NewAdapter(&mocks.MockFilter{
		ContainsCall: func(data string) bool {
			if data == featureName1+key || data == featureName2+key {
				return true
			}

			return false
		},
	})

	if !adapter.Contains(featureName1, key) {
		t.Error("Should return true")
	}

	if !adapter.Contains(featureName2, key) {
		t.Error("Should return true")
	}

	if adapter.Contains("feature-name-not-expected", key) {
		t.Error("Should return false")
	}
}
