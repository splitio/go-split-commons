package telemetry

import (
	"testing"
	"time"
)

func TestBucket(t *testing.T) {
	if Bucket(int64(500*time.Nanosecond)) != 0 {
		t.Error("It should be zero")
	}

	if Bucket(int64(1500*time.Nanosecond)) != 1 {
		t.Error("It should be one")
	}

	if Bucket(int64(8000*time.Nanosecond)) != 6 {
		t.Error("It should be 6")
	}
}
