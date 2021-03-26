package telemetry

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v3/conf"
	"github.com/splitio/go-split-commons/v3/dtos"
	"github.com/splitio/go-split-commons/v3/service/api"
	"github.com/splitio/go-split-commons/v3/service/mocks"
	st "github.com/splitio/go-split-commons/v3/storage/mocks"
	"github.com/splitio/go-toolkit/v4/datastructures/set"
	"github.com/splitio/go-toolkit/v4/logging"
)

func TestTelemetryRecorder(t *testing.T) {
	before := time.Now().UTC().UnixNano() / int64(time.Millisecond)
	mockedSplitStorage := st.MockSplitStorage{
		SplitNamesCall:   func() []string { return []string{} },
		SegmentNamesCall: func() *set.ThreadUnsafeSet { return set.NewSet() },
	}
	mockedSegmentStorage := st.MockSegmentStorage{
		SegmentKeysCountCall: func() int64 { return 10 },
	}
	mockedTelemetryStorage := st.MockTelemetryStorage{
		PopLatenciesCall:           func() dtos.MethodLatencies { return dtos.MethodLatencies{} },
		PopExceptionsCall:          func() dtos.MethodExceptions { return dtos.MethodExceptions{} },
		GetLastSynchronizationCall: func() dtos.LastSynchronization { return dtos.LastSynchronization{} },
		PopHTTPErrorsCall:          func() dtos.HTTPErrors { return dtos.HTTPErrors{} },
		PopHTTPLatenciesCall:       func() dtos.HTTPLatencies { return dtos.HTTPLatencies{} },
		GetImpressionsStatsCall:    func(dataType int) int64 { return 0 },
		GetEventsStatsCall:         func(dataType int) int64 { return 0 },
		PopTokenRefreshesCall:      func() int64 { return 0 },
		PopAuthRejectionsCall:      func() int64 { return 0 },
		PopStreamingEventsCall:     func() []dtos.StreamingEvent { return []dtos.StreamingEvent{} },
		GetSessionLengthCall:       func() int64 { return 0 },
		PopTagsCall:                func() []string { return []string{} },
		RecordSuccessfulSyncCall: func(resource int, tm int64) {
			if resource != TelemetrySync {
				t.Error("Resource should be telemetry")
			}
			if tm < before {
				t.Error("It should be higher than before")
			}
		},
	}

	telemetryRecorderMock := mocks.MockTelemetryRecorder{
		RecordStatsCall: func(stats dtos.Stats, metadata dtos.Metadata) error { return nil },
	}

	telemetrySync := NewTelemetrySynchronizer(mockedTelemetryStorage, telemetryRecorderMock, mockedSplitStorage, mockedSegmentStorage, logging.NewLogger(&logging.LoggerOptions{}), dtos.Metadata{}, mockedTelemetryStorage)

	err := telemetrySync.SynchronizeStats()
	if err != nil {
		t.Error("It should not return err")
	}
}

func TestTelemetryRecorderSync(t *testing.T) {
	before := time.Now().UTC().UnixNano() / int64(time.Millisecond)
	var requestReceived int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics/usage" || r.Method != "POST" {
			t.Error("Invalid request. Should be POST to /metrics")
		}
		atomic.AddInt64(&requestReceived, 1)

		body, err := ioutil.ReadAll(r.Body)
		r.Body.Close()
		if err != nil {
			t.Error("Error reading body")
			return
		}

		var stats dtos.Stats
		err = json.Unmarshal(body, &stats)
		if err != nil {
			t.Errorf("Error parsing json: %s", err)
			return
		}

		if stats.AuthRejections != 10 {
			t.Error("Wrong value sent")
		}
	}))
	defer ts.Close()

	logger := logging.NewLogger(&logging.LoggerOptions{})
	httpTelemetryRecorder := api.NewHTTPTelemetryRecorder(
		"",
		conf.AdvancedConfig{
			TelemetryServiceURL: ts.URL,
		},
		logger,
	)

	mockedSplitStorage := st.MockSplitStorage{
		SplitNamesCall:   func() []string { return []string{} },
		SegmentNamesCall: func() *set.ThreadUnsafeSet { return set.NewSet() },
	}
	mockedSegmentStorage := st.MockSegmentStorage{
		SegmentKeysCountCall: func() int64 { return 10 },
	}
	mockedTelemetryStorage := st.MockTelemetryStorage{
		PopLatenciesCall:           func() dtos.MethodLatencies { return dtos.MethodLatencies{} },
		PopExceptionsCall:          func() dtos.MethodExceptions { return dtos.MethodExceptions{} },
		GetLastSynchronizationCall: func() dtos.LastSynchronization { return dtos.LastSynchronization{} },
		PopHTTPErrorsCall:          func() dtos.HTTPErrors { return dtos.HTTPErrors{} },
		PopHTTPLatenciesCall:       func() dtos.HTTPLatencies { return dtos.HTTPLatencies{} },
		GetImpressionsStatsCall:    func(dataType int) int64 { return 0 },
		GetEventsStatsCall:         func(dataType int) int64 { return 0 },
		PopTokenRefreshesCall:      func() int64 { return 0 },
		PopAuthRejectionsCall:      func() int64 { return 10 },
		PopStreamingEventsCall:     func() []dtos.StreamingEvent { return []dtos.StreamingEvent{} },
		GetSessionLengthCall:       func() int64 { return 0 },
		PopTagsCall:                func() []string { return []string{} },
		RecordSuccessfulSyncCall: func(resource int, tm int64) {
			if resource != TelemetrySync {
				t.Error("Resource should be telemetry")
			}
			if tm < before {
				t.Error("It should be higher than before")
			}
		},
	}

	telemetryRecorder := NewTelemetrySynchronizer(mockedTelemetryStorage, httpTelemetryRecorder, mockedSplitStorage, mockedSegmentStorage, logging.NewLogger(&logging.LoggerOptions{}), dtos.Metadata{}, mockedTelemetryStorage)

	telemetryRecorder.SynchronizeStats()

	if requestReceived != 1 {
		t.Error("It should be called once")
	}
}

func TestConfig(t *testing.T) {
	before := time.Now().UTC().UnixNano() / int64(time.Millisecond)
	called := 0
	logger := logging.NewLogger(&logging.LoggerOptions{})

	mockTelemetryStorage := st.MockTelemetryStorage{
		GetBURTimeoutsCall:    func() int64 { return 3 },
		GetNonReadyUsagesCall: func() int64 { return 5 },
		RecordSuccessfulSyncCall: func(resource int, tm int64) {
			if resource != TelemetrySync {
				t.Error("Resource should be telemetry")
			}
			if tm < before {
				t.Error("It should be higher than before")
			}
		},
	}

	mockRecorder := mocks.MockTelemetryRecorder{
		RecordConfigCall: func(configData dtos.Config, metadata dtos.Metadata) error {
			called++
			if configData.ActiveFactories != 2 {
				t.Error("It should be 2")
			}
			if configData.OperationMode != Standalone {
				t.Error("It should be Standalone")
			}
			if configData.Storage != Memory {
				t.Error("It should be memory")
			}
			if len(configData.Tags) != 0 {
				t.Error("It should be zero")
			}
			if configData.TimeUntilReady != 123456789 {
				t.Error("It should be 123456789")
			}
			return nil
		},
	}

	sync := NewTelemetrySynchronizer(mockTelemetryStorage, mockRecorder, st.MockSplitStorage{}, st.MockSegmentStorage{}, logger, dtos.Metadata{SDKVersion: "go-test", MachineIP: "1.1.1.1", MachineName: "some"}, mockTelemetryStorage)
	factories := make(map[string]int64)
	factories["one"] = 1
	factories["two"] = 1
	sync.SynchronizeConfig(InitConfig{ManagerConfig: conf.ManagerConfig{ImpressionsMode: conf.ImpressionsModeDebug}}, 123456789, factories, []string{})
	if called != 1 {
		t.Error("It should be called once")
	}
}
