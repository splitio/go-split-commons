package strategy

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v4/conf"
	"github.com/splitio/go-split-commons/v4/dtos"
	"github.com/splitio/go-split-commons/v4/storage/inmemory"
	"github.com/splitio/go-split-commons/v4/util"
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

	toLog, toListener = optimized.Apply([]dtos.Impression{imp})
	if len(toLog) != 0 || len(toListener) != 1 {
		t.Error("Should not have to log")
	}
}

func TestApplySingleOptimized(t *testing.T) {
	observer, _ := NewImpressionObserver(5000)
	counter := NewImpressionsCounter()
	runtimeTelemetry, _ := inmemory.NewTelemetryStorage()
	optimized := NewOptimizedImpl(observer, counter, runtimeTelemetry, true)
	featureName := "feature-test"
	timeI := time.Now().UTC().UnixNano()
	key := Key{
		FeatureName: featureName,
		TimeFrame:   util.TruncateTimeFrame(timeI),
	}
	imp := dtos.Impression{
		BucketingKey: "someBuck",
		ChangeNumber: 123,
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         timeI,
		Treatment:    "on",
		FeatureName:  featureName,
		Strategy:     util.ImpressionModeShortVersion(conf.ImpressionsModeDebug),
	}

	toLog := optimized.ApplySingle(&imp)
	if !toLog {
		t.Error("Should be true")
	}

	toLog = optimized.ApplySingle(&imp)
	if toLog {
		t.Error("Should be false")
	}

	imp.Strategy = util.ImpressionModeShortVersion(conf.ImpressionsModeOptimized)

	toLog = optimized.ApplySingle(&imp)
	if toLog {
		t.Error("Should be false")
	}

	counts := counter.PopAll()
	if counts[key] != 2 {
		t.Error("Count should be 2.")
	}
}
