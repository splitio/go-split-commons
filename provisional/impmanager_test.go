package provisional

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/provisional/strategy"
	"github.com/splitio/go-split-commons/v6/storage/filter"
	"github.com/splitio/go-split-commons/v6/storage/inmemory"
	"github.com/splitio/go-split-commons/v6/telemetry"
)

func TestImpManagerInMemoryDebugListenerDisabled(t *testing.T) {
	observer, _ := strategy.NewImpressionObserver(5000)
	debug := strategy.NewDebugImpl(observer, false)
	impManager := NewImpressionManager(debug)

	now := time.Now().UTC().UnixNano()
	imp1 := &dtos.Impression{
		BucketingKey: "someBucketingKey",
		ChangeNumber: 123456789,
		FeatureName:  "someFeature",
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         now,
		Treatment:    "someTreatment",
	}

	impressionsForLog, impressionsForListener := impManager.ProcessImpressions([]dtos.Impression{*imp1})
	if len(impressionsForListener) != 0 {
		t.Error("It should not return an impression")
	}
	if len(impressionsForLog) != 1 {
		t.Error("It should return an impression")
	}
	if impressionsForLog[0].Pt != 0 {
		t.Error("It should not have pt associated yet")
	}

	impressionsForLog, impressionsForListener = impManager.ProcessImpressions([]dtos.Impression{*imp1})
	if len(impressionsForListener) != 0 || len(impressionsForLog) != 1 {
		t.Error("It should return an impression")
	}
}

func TestImpManagerInMemoryDebug(t *testing.T) {
	observer, _ := strategy.NewImpressionObserver(5000)
	debug := strategy.NewDebugImpl(observer, true)
	impManager := NewImpressionManager(debug)

	now := time.Now().UTC().UnixNano()
	imp1 := &dtos.Impression{
		BucketingKey: "someBucketingKey",
		ChangeNumber: 123456789,
		FeatureName:  "someFeature",
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         now,
		Treatment:    "someTreatment",
	}

	impressionsForLog, impressionsForListener := impManager.ProcessImpressions([]dtos.Impression{*imp1})
	if len(impressionsForListener) != 1 || len(impressionsForLog) != 1 {
		t.Error("It should return an impression")
	}
	if impressionsForListener[0].Pt != 0 {
		t.Error("It should not have pt associated yet")
	}

	impressionsForLog, impressionsForListener = impManager.ProcessImpressions([]dtos.Impression{*imp1})
	if len(impressionsForListener) != 1 || len(impressionsForLog) != 1 {
		t.Error("It should return an impression")
	}
	if impressionsForListener[0].Pt != now {
		t.Error("It should have pt associated")
	}
}

func TestImpManagerInMemoryOptimized(t *testing.T) {
	runtimeTelemetry, _ := inmemory.NewTelemetryStorage()
	counter := strategy.NewImpressionsCounter()
	observer, _ := strategy.NewImpressionObserver(5000)
	optimized := strategy.NewOptimizedImpl(observer, counter, runtimeTelemetry, true)
	impManager := NewImpressionManager(optimized)

	now := time.Now().UTC().UnixNano()
	imp1 := &dtos.Impression{
		BucketingKey: "someBucketingKey",
		ChangeNumber: 123456789,
		FeatureName:  "someFeature",
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         now,
		Treatment:    "someTreatment",
	}

	impressionsForLog, impressionsForListener := impManager.ProcessImpressions([]dtos.Impression{*imp1})
	if len(impressionsForListener) != 1 || len(impressionsForLog) != 1 {
		t.Error("It should return an impression")
	}
	if impressionsForListener[0].Pt != 0 {
		t.Error("It should not have pt associated yet")
	}

	impressionsForLog, impressionsForListener = impManager.ProcessImpressions([]dtos.Impression{*imp1})
	if len(impressionsForListener) != 1 {
		t.Error("It should return an impression")
	}
	if len(impressionsForLog) != 0 {
		t.Error("It should not return an impression")
	}
	if impressionsForListener[0].Pt != now {
		t.Error("It should have pt associated")
	}

	if runtimeTelemetry.GetImpressionsStats(telemetry.ImpressionsDeduped) != 1 {
		t.Error("It should be 1")
	}
}

func TestImpManagerInMemoryNone(t *testing.T) {
	counter := strategy.NewImpressionsCounter()
	filter := filter.NewBloomFilter(3000, 0.01)
	uniqueTracker := strategy.NewUniqueKeysTracker(filter)
	none := strategy.NewNoneImpl(counter, uniqueTracker, true)
	impManager := NewImpressionManager(none)

	now := time.Now().UTC().UnixNano()
	imp1 := &dtos.Impression{
		BucketingKey: "someBucketingKey",
		ChangeNumber: 123456789,
		FeatureName:  "someFeature",
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         now,
		Treatment:    "someTreatment",
	}

	impressionsForLog, impressionsForListener := impManager.ProcessImpressions([]dtos.Impression{*imp1})
	if len(impressionsForListener) != 1 {
		t.Error("It should return an impression")
	}
	if len(impressionsForLog) != 0 {
		t.Error("It should not return an impression")
	}

	if impressionsForListener[0].Pt != 0 {
		t.Error("It should not have pt associated")
	}
}

func TestImpManagerRedis(t *testing.T) {
	observer, _ := strategy.NewImpressionObserver(5000)
	debug := strategy.NewDebugImpl(observer, true)
	impManager := NewImpressionManager(debug)

	now := time.Now().UTC().UnixNano()
	imp1 := &dtos.Impression{
		BucketingKey: "someBucketingKey",
		ChangeNumber: 123456789,
		FeatureName:  "someFeature",
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         now,
		Treatment:    "someTreatment",
	}

	impressionsForLog, impressionsForListener := impManager.ProcessImpressions([]dtos.Impression{*imp1})
	if len(impressionsForListener) != 1 || len(impressionsForLog) != 1 {
		t.Error("It should return an impression")
	}
	if impressionsForListener[0].Pt != 0 {
		t.Error("It should not have pt associated")
	}

	impressionsForLog, impressionsForListener = impManager.ProcessImpressions([]dtos.Impression{*imp1})
	if len(impressionsForListener) != 1 {
		t.Error("It should return an impression")
	}
	if len(impressionsForLog) != 1 {
		t.Error("It should return an impression")
	}
	if impressionsForListener[0].Pt == 0 {
		t.Error("It should have pt")
	}
}

func TestProcess(t *testing.T) {
	observer, _ := strategy.NewImpressionObserver(5000)
	debug := strategy.NewDebugImpl(observer, true)
	filter := filter.NewBloomFilter(3000, 0.01)
	uniqueTracker := strategy.NewUniqueKeysTracker(filter)
	counter := strategy.NewImpressionsCounter()
	none := strategy.NewNoneImpl(counter, uniqueTracker, false)

	now := time.Now().UTC().UnixNano()
	values := []dtos.ImpressionDecorated{
		{
			Disabled: true,
			Impression: dtos.Impression{
				BucketingKey: "someBucketingKey",
				ChangeNumber: 123456789,
				FeatureName:  "someFeature",
				KeyName:      "someKey",
				Label:        "someLabel",
				Time:         now,
				Treatment:    "someTreatment",
			},
		},
		{
			Disabled: true,
			Impression: dtos.Impression{
				BucketingKey: "someBucketingKey",
				ChangeNumber: 123456789,
				FeatureName:  "harnessFlag",
				KeyName:      "someKey",
				Label:        "someLabel",
				Time:         now,
				Treatment:    "someTreatment",
			},
		},
		{
			Disabled: false,
			Impression: dtos.Impression{
				BucketingKey: "someBucketingKey",
				ChangeNumber: 123456789,
				FeatureName:  "featureTest",
				KeyName:      "someKey",
				Label:        "someLabel",
				Time:         now,
				Treatment:    "someTreatment",
			},
		},
	}

	impManager := NewImpressionManagerImp(none, debug)
	impressionsForLog, impressionsForListener := impManager.Process(values, true)
	if len(impressionsForListener) != 3 {
		t.Error("Impressions for Listener should be 3. Actual: ", len(impressionsForListener))
	}
	if len(impressionsForLog) != 1 {
		t.Error("Impressions for Log should be 3. Actual: ", len(impressionsForLog))
	}

	impManager = NewImpressionManagerImp(none, none)

	impressionsForLog, impressionsForListener = impManager.Process(values, false)
	if len(impressionsForListener) != 0 {
		t.Error("Impressions for Listener should be 0. Actual: ", len(impressionsForListener))
	}
	if len(impressionsForLog) != 0 {
		t.Error("Impressions for Log should be 1. Actual: ", len(impressionsForLog))
	}
}
