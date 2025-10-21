package tasks

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v8/dtos"
	"github.com/splitio/go-split-commons/v8/provisional/strategy"
	"github.com/splitio/go-split-commons/v8/service/mocks"
	st "github.com/splitio/go-split-commons/v8/storage/mocks"
	"github.com/splitio/go-split-commons/v8/telemetry"
	"github.com/splitio/go-toolkit/v5/datastructures/set"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestUniqueKeysTask(t *testing.T) {
	var call int64

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
		RecordSuccessfulSyncCall:   func(resource int, tm time.Time) {},
		RecordSyncLatencyCall:      func(resource int, latency time.Duration) {},
		RecordUniqueKeysCall: func(uniques dtos.Uniques) error {
			return nil
		},
	}
	mockedTelemetryHTTP := mocks.MockTelemetryRecorder{
		RecordStatsCall: func(stats dtos.Stats, metadata dtos.Metadata) error {
			return nil
		},
		RecordUniqueKeysCall: func(uniques dtos.Uniques, metadata dtos.Metadata) error {
			if len(uniques.Keys) != 2 {
				t.Error("Should be 2")
			}
			call++
			return nil
		},
	}
	mockedSplitStorage := st.MockSplitStorage{
		SplitNamesCall:   func() []string { return []string{} },
		SegmentNamesCall: func() *set.ThreadUnsafeSet { return set.NewSet() },
	}
	mockedSegmentStorage := st.MockSegmentStorage{
		SegmentKeysCountCall: func() int64 { return 10 },
	}

	synchronizer := telemetry.NewTelemetrySynchronizer(
		mockedTelemetryStorage,
		mockedTelemetryHTTP,
		mockedSplitStorage,
		mockedSegmentStorage,
		logging.NewLogger(&logging.LoggerOptions{}),
		dtos.Metadata{},
		mockedTelemetryStorage,
	)

	filter := st.MockFilter{
		AddCall:      func(data string) {},
		ContainsCall: func(data string) bool { return false },
		ClearCall:    func() {},
	}
	tracker := strategy.NewUniqueKeysTracker(filter)
	task := NewRecordUniqueKeysTask(
		synchronizer,
		tracker,
		2,
		logging.NewLogger(&logging.LoggerOptions{}),
	)

	if !tracker.Track("tratment-1", "key-1") {
		t.Error("Should be true")
	}
	if !tracker.Track("tratment-1", "key-2") {
		t.Error("Should be true")
	}
	if !tracker.Track("tratment-2", "key-1") {
		t.Error("Should be true")
	}
	if !tracker.Track("tratment-2", "key-2") {
		t.Error("Should be true")
	}

	task.Start()
	time.Sleep(3 * time.Second)

	if !task.IsRunning() {
		t.Error("UniqueKeys task should be running")
	}

	task.Stop(true)
	if call != 1 {
		t.Error("Request not received")
	}

	if task.IsRunning() {
		t.Error("Task should be stopped")
	}
}
