package strategy

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-split-commons/v5/storage/inmemory"
)

func TestOptimizedMode(t *testing.T) {
	observer, _ := NewImpressionObserver(5000)
	counter := NewImpressionsCounter()
	runtimeTelemetry, _ := inmemory.NewTelemetryStorage()
	optimized := NewOptimizedImpl(observer, counter, runtimeTelemetry, true)
	imp := dtos.Impression{
		BucketingKey: "someBuck",
		ChangeNumber: 123,
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         time.Now().UTC().UnixNano(),
		Treatment:    "on",
		FeatureName:  "feature-test",
	}

	toLog, toListener := optimized.Apply([]dtos.Impression{imp})

	if len(toLog) != 1 || len(toListener) != 1 {
		t.Error("Should have 1 to log")
	}

	if len(counter.impressionsCounts) != 0 {
		t.Error("Should not have counts")
	}

	toLog, toListener = optimized.Apply([]dtos.Impression{imp})

	if len(toLog) != 0 || len(toListener) != 1 {
		t.Error("Should not have to log")
	}

	rawCounts := counter.PopAll()
	if len(rawCounts) != 1 {
		t.Error("Should have counts")
	}
	for key, counts := range counter.PopAll() {
		if key.FeatureName != "feature-test" {
			t.Error("Feature should be feature-test")
		}
		if counts != 1 {
			t.Error("It should be tracked only once")
		}
	}
}

func TestApplySingleOptimized(t *testing.T) {
	observer, _ := NewImpressionObserver(5000)
	counter := NewImpressionsCounter()
	runtimeTelemetry, _ := inmemory.NewTelemetryStorage()
	optimized := NewOptimizedImpl(observer, counter, runtimeTelemetry, true)
	imp := dtos.Impression{
		BucketingKey: "someBuck",
		ChangeNumber: 123,
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         time.Now().UTC().UnixNano(),
		Treatment:    "on",
		FeatureName:  "feature-test",
	}

	toLog := optimized.ApplySingle(&imp)

	if !toLog {
		t.Error("Should be true")
	}

	toLog = optimized.ApplySingle(&imp)

	if toLog {
		t.Error("Should be false")
	}
}
