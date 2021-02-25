package provisional

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v3/conf"
	"github.com/splitio/go-split-commons/v3/dtos"
)

func TestProcessImpressionAllDisabled(t *testing.T) {
	observer, _ := NewImpressionObserver(5000)
	impManagerImpl := ImpressionManagerImpl{
		impressionObserver:    observer,
		impressionsCounter:    NewImpressionsCounter(),
		isOptimized:           false,
		shouldAddPreviousTime: false,
		listenerEnabled:       true,
	}

	now := time.Now().UTC().UnixNano()
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
	forLog, forListener = impManagerImpl.processImpression(imp1, forLog, forListener)

	if len(forLog) != 1 || len(forListener) != 1 {
		t.Error("It should add impression")
	}

	forLog, forListener = impManagerImpl.processImpression(imp1, forLog, forListener)
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
		listenerEnabled:       true,
	}

	now := time.Now().UTC().UnixNano()
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
	forLog, forListener = impManagerImpl.processImpression(imp1, forLog, forListener)

	if len(forLog) != 1 || len(forListener) != 1 {
		t.Error("It should add impression")
	}

	forLog, forListener = impManagerImpl.processImpression(imp1, forLog, forListener)
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
		listenerEnabled:       true,
	}

	now := time.Now().UTC().UnixNano()
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
	forLog, forListener = impManagerImpl.processImpression(imp1, forLog, forListener)

	if len(forLog) != 1 || len(forListener) != 1 {
		t.Error("It should add impression")
	}

	forLog, forListener = impManagerImpl.processImpression(imp1, forLog, forListener)
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

func TestImpManagerInMemoryDebugListenerDisabled(t *testing.T) {
	impManager, err := NewImpressionManager(conf.ManagerConfig{
		OperationMode:   conf.Standalone,
		ImpressionsMode: conf.ImpressionsModeDebug,
		ListenerEnabled: false,
	}, NewImpressionsCounter())
	if err != nil {
		t.Error("It should not return err")
	}

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
	impManager, err := NewImpressionManager(conf.ManagerConfig{
		OperationMode:   conf.Standalone,
		ImpressionsMode: conf.ImpressionsModeDebug,
		ListenerEnabled: true,
	}, NewImpressionsCounter())
	if err != nil {
		t.Error("It should not return err")
	}

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
	impManager, err := NewImpressionManager(conf.ManagerConfig{
		OperationMode:   conf.Standalone,
		ImpressionsMode: conf.ImpressionsModeOptimized,
		ListenerEnabled: true,
	}, NewImpressionsCounter())
	if err != nil {
		t.Error("It should not return err")
	}

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
}

func TestImpManagerRedis(t *testing.T) {
	impManager, err := NewImpressionManager(conf.ManagerConfig{
		OperationMode:   "redis-consumer",
		ImpressionsMode: conf.ImpressionsModeDebug,
		ListenerEnabled: true,
	}, NewImpressionsCounter())
	if err != nil {
		t.Error("It should not return err")
	}

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
	if impressionsForListener[0].Pt != 0 {
		t.Error("It should not have pt")
	}
}
