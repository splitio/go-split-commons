package impression

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service/api"
	recorderMock "github.com/splitio/go-split-commons/service/mocks"
	storageMock "github.com/splitio/go-split-commons/storage/mocks"
	"github.com/splitio/go-split-commons/storage/mutexqueue"
	"github.com/splitio/go-toolkit/logging"
)

func TestImpressionRecorderError(t *testing.T) {
	impressionMockStorage := storageMock.MockImpressionStorage{
		PopNCall: func(n int64) ([]dtos.Impression, error) {
			if n != 50 {
				t.Error("Wrong input parameter passed")
			}
			return make([]dtos.Impression, 0), errors.New("Some")
		},
	}

	impressionMockRecorder := recorderMock.MockImpressionRecorder{}

	impressionSync := NewRecorderSingle(
		impressionMockStorage,
		impressionMockRecorder,
		storageMock.MockMetricStorage{},
		logging.NewLogger(&logging.LoggerOptions{}),
		dtos.Metadata{},
	)

	err := impressionSync.SynchronizeImpressions(50)
	if err == nil {
		t.Error("It should return err")
	}
}

func TestImpressionRecorderWithoutImpressions(t *testing.T) {
	impressionMockStorage := storageMock.MockImpressionStorage{
		PopNCall: func(n int64) ([]dtos.Impression, error) {
			if n != 50 {
				t.Error("Wrong input parameter passed")
			}
			return make([]dtos.Impression, 0), nil
		},
	}

	impressionMockRecorder := recorderMock.MockImpressionRecorder{}

	impressionSync := NewRecorderSingle(
		impressionMockStorage,
		impressionMockRecorder,
		storageMock.MockMetricStorage{},
		logging.NewLogger(&logging.LoggerOptions{}),
		dtos.Metadata{},
	)

	err := impressionSync.SynchronizeImpressions(50)
	if err != nil {
		t.Error("It should not return err")
	}
}

func TestImpressionRecorder(t *testing.T) {
	impression1 := dtos.Impression{
		BucketingKey: "someBucketingKey1",
		ChangeNumber: 123456789,
		FeatureName:  "someFeature1",
		KeyName:      "someKey1",
		Label:        "someLabel",
		Time:         123456789,
		Treatment:    "someTreatment1",
	}
	impression2 := dtos.Impression{
		BucketingKey: "someBucketingKey2",
		ChangeNumber: 123456789,
		FeatureName:  "someFeature2",
		KeyName:      "someKey2",
		Label:        "someLabel",
		Time:         123456789,
		Treatment:    "someTreatment2",
	}

	impressionMockStorage := storageMock.MockImpressionStorage{
		PopNCall: func(n int64) ([]dtos.Impression, error) {
			if n != 50 {
				t.Error("Wrong input parameter passed")
			}
			return []dtos.Impression{impression1, impression2}, nil
		},
	}

	impressionMockRecorder := recorderMock.MockImpressionRecorder{
		RecordCall: func(impressions []dtos.Impression, metadata dtos.Metadata) error {
			if len(impressions) != 2 {
				t.Error("Wrong length of impressions passed")
			}
			if impressions[0].KeyName != "someKey1" {
				t.Error("Wrong impression received")
			}
			if impressions[1].KeyName != "someKey2" {
				t.Error("Wrong impression received")
			}
			return nil
		},
	}

	impressionSync := NewRecorderSingle(
		impressionMockStorage,
		impressionMockRecorder,
		storageMock.MockMetricStorage{
			IncCounterCall: func(key string) {
				if key != "testImpressions.status.200" && key != "backend::request.ok" {
					t.Error("Unexpected counter key to increase")
				}
			},
			IncLatencyCall: func(metricName string, index int) {
				if metricName != "testImpressions.time" && metricName != "backend::/api/testImpressions/bulk" {
					t.Error("Unexpected latency key to track")
				}
			},
		},
		logging.NewLogger(&logging.LoggerOptions{}),
		dtos.Metadata{},
	)

	err := impressionSync.SynchronizeImpressions(50)
	if err != nil {
		t.Error("It should not return err")
	}
}

func TestImpressionRecorderSync(t *testing.T) {
	var requestReceived int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/impressions" && r.Method != "POST" {
			t.Error("Invalid request. Should be POST to /impressions")
		}
		atomic.AddInt64(&requestReceived, 1)

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

		result := make(map[string]dtos.ImpressionsDTO, 0)
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
	impressionRecorder := api.NewHTTPImpressionRecorder(
		"",
		&conf.AdvancedConfig{
			EventsURL: ts.URL,
			SdkURL:    ts.URL,
		},
		logger,
	)

	impression1 := dtos.Impression{
		BucketingKey: "someBucketingKey1",
		ChangeNumber: 123456789,
		FeatureName:  "someFeature1",
		KeyName:      "someKey1",
		Label:        "someLabel",
		Time:         123456789,
		Treatment:    "someTreatment1",
	}
	impression2 := dtos.Impression{
		BucketingKey: "someBucketingKey2",
		ChangeNumber: 123456789,
		FeatureName:  "someFeature2",
		KeyName:      "someKey2",
		Label:        "someLabel",
		Time:         123456789,
		Treatment:    "someTreatment2",
	}
	impression3 := dtos.Impression{
		BucketingKey: "someBucketingKey3",
		ChangeNumber: 123456789,
		FeatureName:  "someFeature1",
		KeyName:      "someKey3",
		Label:        "someLabel",
		Time:         123456789,
		Treatment:    "someTreatment3",
	}

	impressionStorage := mutexqueue.NewMQImpressionsStorage(100, nil, logger)
	impressionStorage.LogImpressions([]dtos.Impression{impression1, impression2, impression3})

	impressionSync := NewRecorderSingle(
		impressionStorage,
		impressionRecorder,
		storageMock.MockMetricStorage{
			IncCounterCall: func(key string) {
				if key != "testImpressions.status.200" && key != "backend::request.ok" {
					t.Error("Unexpected counter key to increase")
				}
			},
			IncLatencyCall: func(metricName string, index int) {
				if metricName != "testImpressions.time" && metricName != "backend::/api/testImpressions/bulk" {
					t.Error("Unexpected latency key to track")
				}
			},
		},
		logging.NewLogger(&logging.LoggerOptions{}),
		dtos.Metadata{},
	)

	impressionSync.SynchronizeImpressions(50)

	if requestReceived != 1 {
		t.Error("It should call once")
	}
}
