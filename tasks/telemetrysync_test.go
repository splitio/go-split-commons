package tasks

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/service/mocks"
	st "github.com/splitio/go-split-commons/v6/storage/mocks"
	"github.com/splitio/go-split-commons/v6/telemetry"
	"github.com/splitio/go-toolkit/v5/datastructures/set"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestTelemetrySyncTask(t *testing.T) {
	var call int64

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
		RecordSuccessfulSyncCall: func(resource int, tm time.Time) {
			if resource != telemetry.TelemetrySync {
				t.Error("Resource should be telemetry")
			}
		},
		RecordSyncLatencyCall: func(resource int, latency time.Duration) {
			if resource != telemetry.TelemetrySync {
				t.Error("Resource should be telemetry")
			}
		},
		PopUpdatesFromSSECall: func() dtos.UpdatesFromSSE { return dtos.UpdatesFromSSE{} },
	}

	mockedTelemetryHTTP := mocks.MockTelemetryRecorder{
		RecordStatsCall: func(stats dtos.Stats, metadata dtos.Metadata) error {
			call++
			return nil
		},
	}

	telemetryTask := NewRecordTelemetryTask(
		telemetry.NewTelemetrySynchronizer(
			mockedTelemetryStorage,
			mockedTelemetryHTTP,
			mockedSplitStorage,
			mockedSegmentStorage,
			logging.NewLogger(&logging.LoggerOptions{}),
			dtos.Metadata{},
			mockedTelemetryStorage,
		),
		2,
		logging.NewLogger(&logging.LoggerOptions{}),
	)

	telemetryTask.Start()
	time.Sleep(3 * time.Second)
	if !telemetryTask.IsRunning() {
		t.Error("Telemetry task should be running")
	}

	telemetryTask.Stop(true)
	if call != 2 {
		t.Error("Request not received")
	}

	if telemetryTask.IsRunning() {
		t.Error("Task should be stopped")
	}
}
