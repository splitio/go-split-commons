package strategy

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v4/dtos"
	"github.com/splitio/go-split-commons/v4/storage/inmemory"
)

func TestOptimizedMode(t *testing.T) {
	observer, _ := NewImpressionObserver(5000)
	counter := NewImpressionsCounter()
	runtimeTelemetry, _ := inmemory.NewTelemetryStorage()
	optimized := NewOptimizedImpl(observer, counter, runtimeTelemetry)
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

	toLog, toListener = optimized.Apply([]dtos.Impression{imp})

	if len(toLog) != 0 || len(toListener) != 1 {
		t.Error("Should not have to log")
	}
}
