package tasks

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v6/conf"
	"github.com/splitio/go-split-commons/v6/dtos"
	recorderMock "github.com/splitio/go-split-commons/v6/service/mocks"
	"github.com/splitio/go-split-commons/v6/storage/mocks"
	"github.com/splitio/go-split-commons/v6/synchronizer/worker/impression"
	"github.com/splitio/go-split-commons/v6/telemetry"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestImpressionSyncTask(t *testing.T) {
	call := 0
	logger := logging.NewLogger(&logging.LoggerOptions{})
	impression1 := dtos.Impression{
		BucketingKey: "someBucketingKey1", ChangeNumber: 123456789, FeatureName: "someFeature1",
		KeyName: "someKey1", Label: "someLabel", Time: 123456789, Treatment: "someTreatment1",
	}
	impression2 := dtos.Impression{
		BucketingKey: "someBucketingKey2", ChangeNumber: 123456789, FeatureName: "someFeature2",
		KeyName: "someKey2", Label: "someLabel", Time: 123456789, Treatment: "someTreatment2",
	}
	impression3 := dtos.Impression{
		BucketingKey: "someBucketingKey3", ChangeNumber: 123456789, FeatureName: "someFeature3",
		KeyName: "someKey3", Label: "someLabel", Time: 123456789, Treatment: "someTreatment3",
	}
	impression4 := dtos.Impression{
		BucketingKey: "someBucketingKey3", ChangeNumber: 123456789, FeatureName: "someFeature2",
		KeyName: "someKey22", Label: "someLabel", Time: 123456789, Treatment: "someTreatment3",
	}

	impressionMockStorage := mocks.MockImpressionStorage{
		PopNCall: func(n int64) ([]dtos.Impression, error) {
			call++
			if n != 50 {
				t.Error("Wrong input parameter passed")
			}
			return []dtos.Impression{impression1, impression2, impression3, impression4}, nil
		},
		EmptyCall: func() bool { return call != 1 },
	}

	impressionMockRecorder := recorderMock.MockImpressionRecorder{
		RecordCall: func(impressions []dtos.ImpressionsDTO, metadata dtos.Metadata, extraHeaders map[string]string) error {
			if len(impressions) != 3 {
				t.Error("Wrong length of impressions passed")
			}
			for _, impression := range impressions {
				switch impression.TestName {
				case "someFeature1":
					if impression.KeyImpressions[0].KeyName != "someKey1" {
						t.Error("Wrong impression received")
					}
				case "someFeature2":
					if len(impression.KeyImpressions) != 2 {
						t.Error("Wrong impressions")
					}
				case "someFeature3":
					if impression.KeyImpressions[0].KeyName != "someKey3" {
						t.Error("Wrong impression received")
					}
				default:
					t.Error("Wrong featureName")
				}
			}
			return nil
		},
	}

	telemetryMockStorage := mocks.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, tm time.Time) {
			if resource != telemetry.ImpressionSync {
				t.Error("Resource should be impressions")
			}
		},
		RecordSyncLatencyCall: func(resource int, tm time.Duration) {
			if resource != telemetry.ImpressionSync {
				t.Error("Resource should be impressions")
			}
		},
	}

	impressionTask := NewRecordImpressionsTask(
		impression.NewRecorderSingle(impressionMockStorage, impressionMockRecorder, logger, dtos.Metadata{}, conf.ImpressionsModeDebug, telemetryMockStorage),
		2,
		logger,
		50,
	)

	impressionTask.Start()
	time.Sleep(3 * time.Second)
	if !impressionTask.IsRunning() {
		t.Error("Impression recorder task should be running")
	}

	impressionTask.Stop(true)
	if impressionTask.IsRunning() {
		t.Error("Task should be stopped")
	}

	time.Sleep(1 * time.Second)
	if call != 2 {
		t.Error("It should call twice for flushing impressions")
	}
}

func TestImpressionSyncTaskMultiple(t *testing.T) {
	var call int64
	logger := logging.NewLogger(&logging.LoggerOptions{})
	impression1 := dtos.Impression{
		BucketingKey: "someBucketingKey1", ChangeNumber: 123456789, FeatureName: "someFeature1",
		KeyName: "someKey1", Label: "someLabel", Time: 123456789, Treatment: "someTreatment1",
	}
	impression2 := dtos.Impression{
		BucketingKey: "someBucketingKey2", ChangeNumber: 123456789, FeatureName: "someFeature2",
		KeyName: "someKey2", Label: "someLabel", Time: 123456789, Treatment: "someTreatment2",
	}
	impression3 := dtos.Impression{
		BucketingKey: "someBucketingKey3", ChangeNumber: 123456789, FeatureName: "someFeature3",
		KeyName: "someKey3", Label: "someLabel", Time: 123456789, Treatment: "someTreatment3",
	}

	impressionMockStorage := mocks.MockImpressionStorage{
		PopNCall: func(n int64) ([]dtos.Impression, error) {
			atomic.AddInt64(&call, 1)
			if n != 50 {
				t.Error("Wrong input parameter passed")
			}
			return []dtos.Impression{impression1, impression2, impression3}, nil
		},
		EmptyCall: func() bool { return call != 1 },
	}

	impressionMockRecorder := recorderMock.MockImpressionRecorder{
		RecordCall: func(impressions []dtos.ImpressionsDTO, metadata dtos.Metadata, extraHeaders map[string]string) error {
			if len(impressions) != 3 {
				t.Error("Wrong length of impressions passed")
			}
			for _, impression := range impressions {
				switch impression.TestName {
				case "someFeature1":
					if impression.KeyImpressions[0].KeyName != "someKey1" {
						t.Error("Wrong impression received")
					}
				case "someFeature2":
					if impression.KeyImpressions[0].KeyName != "someKey2" {
						t.Error("Wrong impression received")
					}
				case "someFeature3":
					if impression.KeyImpressions[0].KeyName != "someKey3" {
						t.Error("Wrong impression received")
					}
				default:
					t.Error("Wrong featureName")
				}
			}
			return nil
		},
	}

	telemetryMockStorage := mocks.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, tm time.Time) {
			if resource != telemetry.ImpressionSync {
				t.Error("Resource should be impressions")
			}
		},
		RecordSyncLatencyCall: func(resource int, tm time.Duration) {
			if resource != telemetry.ImpressionSync {
				t.Error("Resource should be impressions")
			}
		},
	}

	impressionTask := NewRecordImpressionsTasks(
		impression.NewRecorderSingle(impressionMockStorage, impressionMockRecorder, logger, dtos.Metadata{}, conf.ImpressionsModeDebug, telemetryMockStorage),
		2,
		logger,
		50,
		3,
	)

	impressionTask.Start()
	time.Sleep(3 * time.Second)
	if !impressionTask.IsRunning() {
		t.Error("Counter recorder task should be running")
	}
	impressionTask.Stop(true)
	if impressionTask.IsRunning() {
		t.Error("Task should be stopped")
	}

	time.Sleep(1 * time.Second)
	// This task is intended for redis and does not flush on shutdown so it should only execute 3 times.
	if atomic.LoadInt64(&call) != 3 {
		t.Error("It should call three times for sending impressions", call)
	}
}
