package util

import (
	"testing"
	"time"
)

func TestTruncateTimeFrame(t *testing.T) {
	expected := time.Date(2020, 9, 2, 10, 0, 0, 0, time.UTC).UnixNano() / int64(time.Millisecond)

	t1 := TruncateTimeFrame(time.Date(2020, 9, 2, 10, 53, 12, 0, time.UTC).UnixNano())
	if t1 != expected {
		t.Error("Unexpected truncatedTimeFrame", t1, expected)
	}
	t2 := TruncateTimeFrame(time.Date(2020, 9, 2, 10, 0, 0, 0, time.UTC).UnixNano())
	if t2 != expected {
		t.Error("Unexpected truncatedTimeFrame", t2, expected)
	}
	t3 := TruncateTimeFrame(time.Date(2020, 9, 2, 10, 53, 0, 0, time.UTC).UnixNano())
	if t3 != expected {
		t.Error("Unexpected truncatedTimeFrame", t3, expected)
	}
	t4 := TruncateTimeFrame(time.Date(2020, 9, 2, 10, 0, 12, 0, time.UTC).UnixNano())
	if t4 != expected {
		t.Error("Unexpected truncatedTimeFrame", t4, expected)
	}
	expected2 := time.Date(1970, 1, 0, 0, 0, 0, 0, time.UTC).UnixNano() / int64(time.Millisecond)
	t5 := TruncateTimeFrame(time.Date(1970, 1, 0, 0, 0, 0, 0, time.UTC).UnixNano())
	if t5 != expected2 {
		t.Error("Unexpected truncatedTimeFrame", t5, expected2)
	}
}
