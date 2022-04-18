package telemetry

import (
	"testing"
)

func TestBucket(t *testing.T) {
	if Bucket(int64(0)) != 0 {
		t.Error("It should be zero")
	}

	if Bucket(int64(2)) != 2 {
		t.Error("It should be two")
	}

	if Bucket(int64(11)) != 6 {
		t.Error("It should be 6")
	}
}
