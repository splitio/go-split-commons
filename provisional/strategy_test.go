package provisional

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v4/dtos"
	"github.com/splitio/go-split-commons/v4/storage/filter"
	"github.com/splitio/go-split-commons/v4/util"
)

func TestDebugMode(t *testing.T) {
	observer, _ := NewImpressionObserver(5000)
	debug := NewDebugImpl(observer)

	imp := dtos.Impression{
		BucketingKey: "someBuck",
		ChangeNumber: 123,
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         123456,
		Treatment:    "on",
		FeatureName:  "feature-test",
	}

	toLog := debug.Apply(&imp)

	if !toLog {
		t.Error("Should not be true")
	}
}

func TestNoneMode(t *testing.T) {
	now := time.Now().UTC().UnixNano()
	filter := filter.NewBloomFilter(1000, 0.01)
	tracker := NewUniqueKeysTracker(filter)
	counter := NewImpressionsCounter()
	none := NewNoneImpl(counter, tracker)

	imp := dtos.Impression{
		BucketingKey: "someBuck",
		ChangeNumber: 123,
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         now,
		Treatment:    "on",
		FeatureName:  "feature-test",
	}

	result := none.Apply(&imp)

	if result {
		t.Error("Should be false")
	}

	result = none.Apply(&imp)

	if result {
		t.Error("Should be false")
	}

	counts := counter.PopAll()
	value := counts[Key{
		FeatureName: imp.FeatureName,
		TimeFrame:   util.TruncateTimeFrame(now),
	}]

	if value != 2 {
		t.Error("Should be 2")
	}
}

func TestOptimizedMode(t *testing.T) {
	observer, _ := NewImpressionObserver(5000)
	counter := NewImpressionsCounter()
	optimized := NewOptimizedImpl(counter, observer)
	imp := dtos.Impression{
		BucketingKey: "someBuck",
		ChangeNumber: 123,
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         time.Now().UTC().UnixNano(),
		Treatment:    "on",
		FeatureName:  "feature-test",
	}

	result := optimized.Apply(&imp)

	if !result {
		t.Error("Should not be true")
	}

	result2 := optimized.Apply(&imp)

	if result2 {
		t.Error("Should be false")
	}
}
