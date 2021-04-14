package impression

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service/api"
	recorderMock "github.com/splitio/go-split-commons/service/mocks"
	"github.com/splitio/go-split-commons/storage/inmemory"
	"github.com/splitio/go-split-commons/storage/inmemory/mutexqueue"
	"github.com/splitio/go-split-commons/storage/mocks"
	"github.com/splitio/go-split-commons/telemetry"
	"github.com/splitio/go-toolkit/logging"
)

func TestImpressionRecorderError(t *testing.T) {
	impressionMockStorage := mocks.MockImpressionStorage{
		PopNCall: func(n int64) ([]dtos.Impression, error) {
			if n != 50 {
				t.Error("Wrong input parameter passed")
			}
			return make([]dtos.Impression, 0), errors.New("Some")
		},
	}
	impressionMockRecorder := recorderMock.MockImpressionRecorder{}
	telemetryMockStorage := mocks.MockTelemetryStorage{}

	impressionSync := NewRecorderSingle(impressionMockStorage, impressionMockRecorder, logging.NewLogger(&logging.LoggerOptions{}), dtos.Metadata{}, conf.ManagerConfig{ImpressionsMode: conf.ImpressionsModeDebug}, telemetryMockStorage)
	err := impressionSync.SynchronizeImpressions(50)
	if err == nil {
		t.Error("It should return err")
	}
}

func TestImpressionRecorderWithoutImpressions(t *testing.T) {
	impressionMockStorage := mocks.MockImpressionStorage{
		PopNCall: func(n int64) ([]dtos.Impression, error) {
			if n != 50 {
				t.Error("Wrong input parameter passed")
			}
			return make([]dtos.Impression, 0), nil
		},
	}

	impressionMockRecorder := recorderMock.MockImpressionRecorder{}
	telemetryMockStorage := mocks.MockTelemetryStorage{}

	impressionSync := NewRecorderSingle(impressionMockStorage, impressionMockRecorder, logging.NewLogger(&logging.LoggerOptions{}), dtos.Metadata{}, conf.ManagerConfig{ImpressionsMode: conf.ImpressionsModeDebug}, telemetryMockStorage)

	err := impressionSync.SynchronizeImpressions(50)
	if err != nil {
		t.Error("It should not return err")
	}
}

func TestSynhronizeEventErrorRecorder(t *testing.T) {
	impression := dtos.Impression{
		BucketingKey: "someBucketingKey1", ChangeNumber: 123456789, FeatureName: "someFeature1",
		KeyName: "someKey1", Label: "someLabel", Time: 123456789, Treatment: "someTreatment1",
	}

	impressionMockStorage := mocks.MockImpressionStorage{
		PopNCall: func(n int64) ([]dtos.Impression, error) {
			if n != 50 {
				t.Error("Wrong input parameter passed")
			}
			return []dtos.Impression{impression}, nil
		},
	}

	impressionMockRecorder := recorderMock.MockImpressionRecorder{
		RecordCall: func(impressions []dtos.ImpressionsDTO, metadata dtos.Metadata, extraHeaders map[string]string) error {
			return &dtos.HTTPError{Code: 500, Message: "some"}
		},
	}

	telemetryMockStorage := mocks.MockTelemetryStorage{
		RecordSyncErrorCall: func(resource, status int) {
			if resource != telemetry.ImpressionSync {
				t.Error("It should be impressions")
			}
			if status != 500 {
				t.Error("Status should be 500")
			}
		},
	}

	impressionSync := NewRecorderSingle(impressionMockStorage, impressionMockRecorder, logging.NewLogger(&logging.LoggerOptions{}), dtos.Metadata{}, conf.ManagerConfig{ImpressionsMode: conf.ImpressionsModeDebug}, telemetryMockStorage)
	err := impressionSync.SynchronizeImpressions(50)
	if err == nil {
		t.Error("It should return err")
	}
}

func TestImpressionRecorder(t *testing.T) {
	before := time.Now().UTC().UnixNano() / int64(time.Millisecond)
	impression1 := dtos.Impression{
		BucketingKey: "someBucketingKey1", ChangeNumber: 123456789, FeatureName: "someFeature1",
		KeyName: "someKey1", Label: "someLabel", Time: 123456789, Treatment: "someTreatment1",
	}
	impression2 := dtos.Impression{
		BucketingKey: "someBucketingKey2", ChangeNumber: 123456789, FeatureName: "someFeature2",
		KeyName: "someKey2", Label: "someLabel", Time: 123456789, Treatment: "someTreatment2",
	}

	impressionMockStorage := mocks.MockImpressionStorage{
		PopNCall: func(n int64) ([]dtos.Impression, error) {
			if n != 50 {
				t.Error("Wrong input parameter passed")
			}
			return []dtos.Impression{impression1, impression2}, nil
		},
	}

	impressionMockRecorder := recorderMock.MockImpressionRecorder{
		RecordCall: func(impressions []dtos.ImpressionsDTO, metadata dtos.Metadata, extraHeaders map[string]string) error {
			val, ok := extraHeaders[splitSDKImpressionsMode]
			if !ok {
				t.Error("It should send extraHeaders")
			}
			if val != conf.ImpressionsModeDebug {
				t.Error("It should be debug")
			}
			if len(impressions) != 2 {
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
				default:
					t.Error("Wrong featureName")
				}
			}

			return nil
		},
	}

	telemetryMockStorage := mocks.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, tm int64) {
			if resource != telemetry.ImpressionSync {
				t.Error("Resource should be impressions")
			}
			if tm < before {
				t.Error("It should be higher than before")
			}
		},
		RecordSyncLatencyCall: func(resource int, tm int64) {
			if resource != telemetry.ImpressionSync {
				t.Error("Resource should be impresisons")
			}
		},
	}

	impressionSync := NewRecorderSingle(impressionMockStorage, impressionMockRecorder, logging.NewLogger(&logging.LoggerOptions{}), dtos.Metadata{}, conf.ManagerConfig{ImpressionsMode: conf.ImpressionsModeDebug}, telemetryMockStorage)

	err := impressionSync.SynchronizeImpressions(50)
	if err != nil {
		t.Error("It should not return err")
	}
}

func TestImpressionRecorderSync(t *testing.T) {
	var requestReceived int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/testImpressions/bulk" || r.Method != "POST" {
			t.Error("Invalid request. Should be POST to /testImpressions/bulk")
		}
		atomic.AddInt64(&requestReceived, 1)

		if r.Header.Get(splitSDKImpressionsMode) != conf.ImpressionsModeOptimized {
			t.Error("Wrong header sent")
		}

		body, err := ioutil.ReadAll(r.Body)
		r.Body.Close()
		if err != nil {
			t.Error("Error reading body")
			return
		}

		var impressions []dtos.ImpressionsDTO
		err = json.Unmarshal(body, &impressions)
		if err != nil {
			t.Errorf("Error parsing json: %s", err)
			return
		}

		if len(impressions) != 2 {
			t.Error("Incorrect number of features")
			return
		}

		result := make(map[string]dtos.ImpressionsDTO)
		for _, impression := range impressions {
			result[impression.TestName] = impression
		}

		imp1, ok := result["someFeature1"]
		if !ok || len(imp1.KeyImpressions) != 2 {
			t.Error("Incorrect impressions received")
		}
		imp2, ok := result["someFeature2"]
		if !ok || len(imp2.KeyImpressions) != 1 {
			t.Error("Incorrect impressions received")
		}
	}))
	defer ts.Close()

	logger := logging.NewLogger(&logging.LoggerOptions{})
	impressionRecorder := api.NewHTTPImpressionRecorder("", conf.AdvancedConfig{EventsURL: ts.URL}, logger)

	impression1 := dtos.Impression{
		BucketingKey: "someBucketingKey1", ChangeNumber: 123456789, FeatureName: "someFeature1",
		KeyName: "someKey1", Label: "someLabel", Time: 123456789, Treatment: "someTreatment1",
	}
	impression2 := dtos.Impression{
		BucketingKey: "someBucketingKey2", ChangeNumber: 123456789, FeatureName: "someFeature2",
		KeyName: "someKey2", Label: "someLabel", Time: 123456789, Treatment: "someTreatment2",
	}
	impression3 := dtos.Impression{
		BucketingKey: "someBucketingKey3", ChangeNumber: 123456789, FeatureName: "someFeature1",
		KeyName: "someKey3", Label: "someLabel", Time: 123456789, Treatment: "someTreatment3",
	}

	runtimeTelemetry, _ := inmemory.NewTelemetryStorage()
	impressionStorage := mutexqueue.NewMQImpressionsStorage(100, nil, logger, runtimeTelemetry)
	impressionStorage.LogImpressions([]dtos.Impression{impression1, impression2, impression3})

	impressionSync := NewRecorderSingle(impressionStorage, impressionRecorder, logger, dtos.Metadata{}, conf.ManagerConfig{OperationMode: conf.Standalone, ImpressionsMode: conf.ImpressionsModeOptimized}, runtimeTelemetry)

	impressionSync.SynchronizeImpressions(50)

	if requestReceived != 1 {
		t.Error("It should call once")
	}
}

func TestImpressionLastSeen(t *testing.T) {
	var requestReceived int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/testImpressions/bulk" || r.Method != "POST" {
			t.Error("Invalid request. Should be POST to /testImpressions/bulk")
		}
		atomic.AddInt64(&requestReceived, 1)

		if r.Header.Get(splitSDKImpressionsMode) != conf.ImpressionsModeDebug {
			t.Error("Wrong header sent")
		}

		body, err := ioutil.ReadAll(r.Body)
		r.Body.Close()
		if err != nil {
			t.Error("Error reading body")
			return
		}

		var impressions []dtos.ImpressionsDTO
		err = json.Unmarshal(body, &impressions)
		if err != nil {
			t.Errorf("Error parsing json: %s", err)
			return
		}

		if len(impressions) != 1 {
			t.Error("Incorrect number of features")
			return
		}

		result := make(map[string]dtos.ImpressionsDTO)
		for _, impression := range impressions {
			result[impression.TestName] = impression
		}

		imp1, ok := result["someFeature1"]
		if !ok || len(imp1.KeyImpressions) != 1 {
			t.Error("Incorrect impressions received")
			for _, ki := range imp1.KeyImpressions {
				if atomic.LoadInt64(&requestReceived) == 1 {
					if ki.Pt != 0 {
						t.Error("Unexpected lastSeen")
					}
				} else {
					if ki.Pt != 123456789 {
						t.Error("Unexpected lastSeen")
					}
				}
			}
		}
	}))
	defer ts.Close()

	logger := logging.NewLogger(&logging.LoggerOptions{})
	impressionRecorder := api.NewHTTPImpressionRecorder("", conf.AdvancedConfig{EventsURL: ts.URL}, logger)

	impression1 := dtos.Impression{
		BucketingKey: "someBucketingKey1", ChangeNumber: 123456789, FeatureName: "someFeature1",
		KeyName: "someKey1", Label: "someLabel", Time: 123456789, Treatment: "someTreatment1",
	}

	runtimeTelemetry, _ := inmemory.NewTelemetryStorage()
	impressionStorage := mutexqueue.NewMQImpressionsStorage(100, nil, logger, runtimeTelemetry)
	impressionStorage.LogImpressions([]dtos.Impression{impression1})

	impressionSync := NewRecorderSingle(impressionStorage, impressionRecorder, logger, dtos.Metadata{}, conf.ManagerConfig{ImpressionsMode: conf.ImpressionsModeDebug}, runtimeTelemetry)

	impressionSync.SynchronizeImpressions(50)

	impressionStorage.LogImpressions([]dtos.Impression{impression1})
	impressionSync.SynchronizeImpressions(50)
}
