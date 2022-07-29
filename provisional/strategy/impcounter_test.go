package strategy

import (
	"fmt"
	"testing"
	"time"
)

func TestMakeKey(t *testing.T) {
	timestamp := time.Date(2020, 9, 2, 10, 0, 0, 0, time.UTC).UnixNano() / int64(time.Millisecond)

	actualKey := makeKey("someFeature", time.Date(2020, 9, 2, 10, 0, 0, 0, time.UTC).UnixNano()/int64(time.Millisecond))
	expectedKey := Key{FeatureName: "someFeature", TimeFrame: timestamp}
	if actualKey != expectedKey {
		t.Error(fmt.Sprintf("Unexpected key generated %v, %v", actualKey, expectedKey))
	}

	actualKey2 := makeKey("", time.Date(2020, 9, 2, 10, 0, 0, 0, time.UTC).UnixNano()/int64(time.Millisecond))
	expectedKey2 := Key{FeatureName: "", TimeFrame: timestamp}
	if actualKey2 != expectedKey2 {
		t.Error(fmt.Sprintf("Unexpected key generated %v, %v", actualKey2, expectedKey2))
	}

	actualKey3 := makeKey("someFeature", 0)
	expectedKey3 := Key{FeatureName: "someFeature", TimeFrame: 0}
	if actualKey3 != expectedKey3 {
		t.Error(fmt.Sprintf("Unexpected key generated %v, %v", actualKey3, expectedKey3))
	}
}

func TestImpressionsCounter(t *testing.T) {
	timestamp := time.Date(2020, 9, 2, 10, 10, 12, 0, time.UTC).UnixNano()
	impressionsCounter := NewImpressionsCounter()

	impressionsCounter.Inc("feature1", timestamp, 1)
	impressionsCounter.Inc("feature1", timestamp+1, 1)
	impressionsCounter.Inc("feature1", timestamp+2, 1)
	impressionsCounter.Inc("feature2", timestamp+3, 2)
	impressionsCounter.Inc("feature2", timestamp+4, 2)

	counted := impressionsCounter.PopAll()
	if len(counted) != 2 {
		t.Error("It should be 2")
	}
	if counted[makeKey("feature1", timestamp)] != 3 {
		t.Error("It should be 3")
	}
	if counted[makeKey("feature2", timestamp)] != 4 {
		t.Error("It should be 4")
	}
	if len(impressionsCounter.PopAll()) != 0 {
		t.Error("It should not have keys")
	}

	nextHourTimestamp := time.Date(2020, 9, 2, 11, 10, 12, 0, time.UTC).UnixNano()
	impressionsCounter.Inc("feature1", timestamp, 1)
	impressionsCounter.Inc("feature1", timestamp+1, 1)
	impressionsCounter.Inc("feature1", timestamp+2, 1)
	impressionsCounter.Inc("feature2", timestamp+3, 2)
	impressionsCounter.Inc("feature2", timestamp+4, 2)
	impressionsCounter.Inc("feature1", nextHourTimestamp, 1)
	impressionsCounter.Inc("feature1", nextHourTimestamp+1, 1)
	impressionsCounter.Inc("feature1", nextHourTimestamp+2, 1)
	impressionsCounter.Inc("feature2", nextHourTimestamp+3, 2)
	impressionsCounter.Inc("feature2", nextHourTimestamp+4, 2)

	counted = impressionsCounter.PopAll()
	if len(counted) != 4 {
		t.Error("It should be 4")
	}
	if counted[makeKey("feature1", timestamp)] != 3 {
		t.Error("It should be 3")
	}
	if counted[makeKey("feature2", timestamp)] != 4 {
		t.Error("It should be 4")
	}
	if counted[makeKey("feature1", nextHourTimestamp)] != 3 {
		t.Error("It should be 3")
	}
	if counted[makeKey("feature2", nextHourTimestamp)] != 4 {
		t.Error("It should be 4")
	}
	if len(impressionsCounter.PopAll()) != 0 {
		t.Error("It should not have keys")
	}
}
