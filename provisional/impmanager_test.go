package provisional

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-split-commons/dtos"
)

func TestProcessImpressionAllDisabled(t *testing.T) {
	observer, _ := NewImpressionObserver(5000)
	impManagerImpl := ImpressionManagerImpl{
		impressionObserver:    observer,
		impressionsCounter:    NewImpressionsCounter(),
		isOptimized:           false,
		shouldAddPreviousTime: false,
	}

	now := time.Now().UnixNano() / int64(time.Millisecond)
	imp1 := dtos.Impression{
		BucketingKey: "someBucketingKey",
		ChangeNumber: 123456789,
		FeatureName:  "someFeature",
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         now,
		Treatment:    "someTreatment",
	}

	forLog := make([]dtos.Impression, 0)
	forListener := make([]dtos.Impression, 0)
	impManagerImpl.ProcessImpression(imp1, &forLog, &forListener)

	if len(forLog) != 1 || len(forListener) != 1 {
		t.Error("It should add impression")
	}

	impManagerImpl.ProcessImpression(imp1, &forLog, &forListener)
	if len(forLog) != 2 || len(forListener) != 2 {
		t.Error("It should have two impressions")
	}

	if forLog[0].Pt != 0 {
		t.Error("It should not have pt associated yet")
	}
	if forLog[1].Pt != 0 {
		t.Error("It should have pt associated")
	}
}

func TestProcessImpressionOptimizedDisabled(t *testing.T) {
	observer, _ := NewImpressionObserver(5000)
	impManagerImpl := ImpressionManagerImpl{
		impressionObserver:    observer,
		impressionsCounter:    NewImpressionsCounter(),
		isOptimized:           false,
		shouldAddPreviousTime: true,
	}

	now := time.Now().UnixNano() / int64(time.Millisecond)
	imp1 := dtos.Impression{
		BucketingKey: "someBucketingKey",
		ChangeNumber: 123456789,
		FeatureName:  "someFeature",
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         now,
		Treatment:    "someTreatment",
	}

	forLog := make([]dtos.Impression, 0)
	forListener := make([]dtos.Impression, 0)
	impManagerImpl.ProcessImpression(imp1, &forLog, &forListener)

	if len(forLog) != 1 || len(forListener) != 1 {
		t.Error("It should add impression")
	}

	impManagerImpl.ProcessImpression(imp1, &forLog, &forListener)
	if len(forLog) != 2 || len(forListener) != 2 {
		t.Error("It should have two impressions")
	}

	if forLog[0].Pt != 0 {
		t.Error("It should not have pt associated yet")
	}
	if forLog[1].Pt != now {
		t.Error("It should have pt associated")
	}
}

func TestProcessImpressionOptimizedEnabled(t *testing.T) {
	observer, _ := NewImpressionObserver(5000)
	impManagerImpl := ImpressionManagerImpl{
		impressionObserver:    observer,
		impressionsCounter:    NewImpressionsCounter(),
		isOptimized:           true,
		shouldAddPreviousTime: true,
	}

	now := time.Now().UnixNano() / int64(time.Millisecond)
	imp1 := dtos.Impression{
		BucketingKey: "someBucketingKey",
		ChangeNumber: 123456789,
		FeatureName:  "someFeature",
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         now,
		Treatment:    "someTreatment",
	}

	forLog := make([]dtos.Impression, 0)
	forListener := make([]dtos.Impression, 0)
	impManagerImpl.ProcessImpression(imp1, &forLog, &forListener)

	if len(forLog) != 1 || len(forListener) != 1 {
		t.Error("It should add impression")
	}

	impManagerImpl.ProcessImpression(imp1, &forLog, &forListener)
	if len(forLog) != 1 {
		t.Error("It should not add new impression")
	}
	if len(forListener) != 2 {
		t.Error("It should have two impressions")
	}

	if forListener[0].Pt != 0 {
		t.Error("It should not have pt associated yet")
	}
	if forListener[1].Pt != now {
		t.Error("It should have pt associated")
	}
}

func TestImpManagerInMemoryDebug(t *testing.T) {
	impManager, err := NewImpressionManager(conf.ManagerConfig{
		OperationMode:   "inmemory-standalone",
		ImpressionsMode: "DEBUG",
	})
	if err != nil {
		t.Error("It should not return err")
	}

	now := time.Now().UnixNano() / int64(time.Millisecond)
	imp1 := &dtos.Impression{
		BucketingKey: "someBucketingKey",
		ChangeNumber: 123456789,
		FeatureName:  "someFeature",
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         now,
		Treatment:    "someTreatment",
	}

	result := impManager.ProcessImpressions([]dtos.Impression{*imp1})
	if len(result.ImpressionsForListener) != 1 || len(result.ImpressionsForLog) != 1 {
		t.Error("It should return an impression")
	}
	if result.ImpressionsForListener[0].Pt != 0 {
		t.Error("It should not have pt associated yet")
	}

	result2 := impManager.ProcessImpressions([]dtos.Impression{*imp1})
	if len(result2.ImpressionsForListener) != 1 || len(result.ImpressionsForLog) != 1 {
		t.Error("It should return an impression")
	}
	if result2.ImpressionsForListener[0].Pt != now {
		t.Error("It should have pt associated")
	}
}

func TestImpManagerInMemoryOptimized(t *testing.T) {
	impManager, err := NewImpressionManager(conf.ManagerConfig{
		OperationMode:   conf.Standalone,
		ImpressionsMode: conf.Optimized,
	})
	if err != nil {
		t.Error("It should not return err")
	}

	now := time.Now().UnixNano() / int64(time.Millisecond)
	imp1 := &dtos.Impression{
		BucketingKey: "someBucketingKey",
		ChangeNumber: 123456789,
		FeatureName:  "someFeature",
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         now,
		Treatment:    "someTreatment",
	}

	result := impManager.ProcessImpressions([]dtos.Impression{*imp1})
	if len(result.ImpressionsForListener) != 1 || len(result.ImpressionsForLog) != 1 {
		t.Error("It should return an impression")
	}
	if result.ImpressionsForListener[0].Pt != 0 {
		t.Error("It should not have pt associated yet")
	}

	result2 := impManager.ProcessImpressions([]dtos.Impression{*imp1})
	if len(result2.ImpressionsForListener) != 1 {
		t.Error("It should return an impression")
	}
	if len(result2.ImpressionsForLog) != 0 {
		t.Error("It should not return an impression")
	}
	if result2.ImpressionsForListener[0].Pt != now {
		t.Error("It should have pt associated")
	}
}

func TestImpManagerRedis(t *testing.T) {
	impManager, err := NewImpressionManager(conf.ManagerConfig{
		OperationMode:   "redis-consumer",
		ImpressionsMode: conf.Debug,
	})
	if err != nil {
		t.Error("It should not return err")
	}

	now := time.Now().UnixNano() / int64(time.Millisecond)
	imp1 := &dtos.Impression{
		BucketingKey: "someBucketingKey",
		ChangeNumber: 123456789,
		FeatureName:  "someFeature",
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         now,
		Treatment:    "someTreatment",
	}

	result := impManager.ProcessImpressions([]dtos.Impression{*imp1})
	if len(result.ImpressionsForListener) != 1 || len(result.ImpressionsForLog) != 1 {
		t.Error("It should return an impression")
	}
	if result.ImpressionsForListener[0].Pt != 0 {
		t.Error("It should not have pt associated")
	}

	result2 := impManager.ProcessImpressions([]dtos.Impression{*imp1})
	if len(result2.ImpressionsForListener) != 1 {
		t.Error("It should return an impression")
	}
	if len(result2.ImpressionsForLog) != 1 {
		t.Error("It should return an impression")
	}
	if result2.ImpressionsForListener[0].Pt != 0 {
		t.Error("It should not have pt")
	}
}
