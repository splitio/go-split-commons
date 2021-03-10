package inmemory

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v3/dtos"
	"github.com/splitio/go-split-commons/v3/storage"
	"github.com/splitio/go-split-commons/v3/util"
)

func TestTelemetryStorage(t *testing.T) {
	telemetryStorage := NewIMTelemetryStorage()

	telemetryStorage.RecordException(storage.Treatment)
	telemetryStorage.RecordException(storage.Treatments)
	telemetryStorage.RecordException(storage.Treatment)
	telemetryStorage.RecordLatency(storage.Treatment, util.Bucket((1500 * time.Nanosecond).Nanoseconds()))
	telemetryStorage.RecordLatency(storage.Treatment, util.Bucket((2000 * time.Nanosecond).Nanoseconds()))
	telemetryStorage.RecordLatency(storage.Treatments, util.Bucket((3000 * time.Nanosecond).Nanoseconds()))
	telemetryStorage.RecordLatency(storage.Treatments, util.Bucket((500 * time.Nanosecond).Nanoseconds()))
	telemetryStorage.RecordLatency(storage.TreatmentWithConfig, util.Bucket((800 * time.Nanosecond).Nanoseconds()))
	telemetryStorage.RecordLatency(storage.TreatmentsWithConfig, util.Bucket((1000 * time.Nanosecond).Nanoseconds()))

	exceptions := telemetryStorage.PopExceptions()
	if exceptions.Treatment != 2 || exceptions.Treatments != 1 || exceptions.TreatmentWithConfig != 0 || exceptions.TreatmentWithConfigs != 0 || exceptions.Track != 0 {
		t.Error("Wrong result")
	}
	exceptions = telemetryStorage.PopExceptions()
	if exceptions.Treatment != 0 || exceptions.Treatments != 0 || exceptions.TreatmentWithConfig != 0 || exceptions.TreatmentWithConfigs != 0 || exceptions.Track != 0 {
		t.Error("Wrong result")
	}
	latencies := telemetryStorage.PopLatencies()
	if latencies.Treatment[1] != 1 || latencies.Treatment[2] != 1 {
		t.Error("Wrong result")
	}
	if latencies.Treatments[0] != 1 || latencies.Treatments[3] != 1 {
		t.Error("Wrong result")
	}
	if latencies.TreatmentWithConfig[0] != 1 {
		t.Error("Wrong result")
	}
	if latencies.TreatmentWithConfigs[0] != 1 {
		t.Error("Wrong result")
	}
	latencies = telemetryStorage.PopLatencies()
	if latencies.Treatment[1] != 0 {
		t.Error("Wrong result")
	}

	telemetryStorage.RecordImpressionsStats(storage.ImpressionsQueued, 200)
	telemetryStorage.RecordImpressionsStats(storage.ImpressionsDeduped, 100)
	telemetryStorage.RecordImpressionsStats(storage.ImpressionsDropped, 50)
	telemetryStorage.RecordImpressionsStats(storage.ImpressionsQueued, 200)
	if telemetryStorage.GetImpressionsStats(storage.ImpressionsDeduped) != 100 {
		t.Error("Wrong result")
	}
	if telemetryStorage.GetImpressionsStats(storage.ImpressionsQueued) != 400 {
		t.Error("Wrong result")
	}
	if telemetryStorage.GetImpressionsStats(storage.ImpressionsDropped) != 50 {
		t.Error("Wrong result")
	}

	telemetryStorage.RecordEventsStats(storage.EventsDropped, 100)
	telemetryStorage.RecordEventsStats(storage.EventsQueued, 10)
	telemetryStorage.RecordEventsStats(storage.EventsDropped, 100)
	telemetryStorage.RecordEventsStats(storage.EventsQueued, 10)
	if telemetryStorage.GetEventsStats(storage.EventsDropped) != 200 {
		t.Error("Wrong result")
	}
	if telemetryStorage.GetEventsStats(storage.EventsQueued) != 20 {
		t.Error("Wrong result")
	}

	telemetryStorage.RecordSuccessfulSync(storage.SplitSync, time.Now().UnixNano())
	time.Sleep(100 * time.Millisecond)
	telemetryStorage.RecordSuccessfulSync(storage.SegmentSync, time.Now().UnixNano())
	time.Sleep(100 * time.Millisecond)
	telemetryStorage.RecordSuccessfulSync(storage.ImpressionSync, time.Now().UnixNano())
	time.Sleep(100 * time.Millisecond)
	telemetryStorage.RecordSuccessfulSync(storage.EventSync, time.Now().UnixNano())
	time.Sleep(100 * time.Millisecond)
	telemetryStorage.RecordSuccessfulSync(storage.TelemetrySync, time.Now().UnixNano())
	time.Sleep(100 * time.Millisecond)
	telemetryStorage.RecordSuccessfulSync(storage.TokenSync, time.Now().UnixNano())
	lastSynchronization := telemetryStorage.GetLastSynchronization()
	if lastSynchronization.Splits == 0 || lastSynchronization.Segments == 0 || lastSynchronization.Impressions == 0 || lastSynchronization.Events == 0 || lastSynchronization.Telemetry == 0 {
		t.Error("Wrong result")
	}

	telemetryStorage.RecordSyncError(storage.SplitSync, 500)
	telemetryStorage.RecordSyncError(storage.SplitSync, 500)
	telemetryStorage.RecordSyncError(storage.SplitSync, 500)
	telemetryStorage.RecordSyncError(storage.SplitSync, 500)
	telemetryStorage.RecordSyncError(storage.SplitSync, 500)
	telemetryStorage.RecordSyncError(storage.SegmentSync, 401)
	telemetryStorage.RecordSyncError(storage.SegmentSync, 401)
	telemetryStorage.RecordSyncError(storage.SegmentSync, 401)
	telemetryStorage.RecordSyncError(storage.SegmentSync, 404)
	telemetryStorage.RecordSyncError(storage.ImpressionSync, 402)
	telemetryStorage.RecordSyncError(storage.ImpressionSync, 402)
	telemetryStorage.RecordSyncError(storage.ImpressionSync, 402)
	telemetryStorage.RecordSyncError(storage.ImpressionSync, 402)
	telemetryStorage.RecordSyncError(storage.EventSync, 400)
	telemetryStorage.RecordSyncError(storage.TelemetrySync, 401)
	telemetryStorage.RecordSyncError(storage.TokenSync, 400)
	telemetryStorage.RecordSyncLatency(storage.SplitSync, util.Bucket((1500 * time.Nanosecond).Nanoseconds()))
	telemetryStorage.RecordSyncLatency(storage.SplitSync, util.Bucket((3000 * time.Nanosecond).Nanoseconds()))
	telemetryStorage.RecordSyncLatency(storage.SplitSync, util.Bucket((4000 * time.Nanosecond).Nanoseconds()))
	telemetryStorage.RecordSyncLatency(storage.SegmentSync, util.Bucket((1500 * time.Nanosecond).Nanoseconds()))
	telemetryStorage.RecordSyncLatency(storage.SegmentSync, util.Bucket((1500 * time.Nanosecond).Nanoseconds()))

	httpErrors := telemetryStorage.PopHTTPErrors()
	if httpErrors.Splits[500] != 5 || httpErrors.Segments[401] != 3 || httpErrors.Segments[404] != 1 || httpErrors.Impressions[402] != 4 || httpErrors.Events[400] != 1 || httpErrors.Telemetry[401] != 1 || httpErrors.Token[400] != 1 {
		t.Error("Wrong result")
	}
	httpErrors = telemetryStorage.PopHTTPErrors()
	if len(httpErrors.Splits) != 0 || len(httpErrors.Segments) != 0 { // and so on
		t.Error("Wrong result")
	}

	httpLatencies := telemetryStorage.PopHTTPLatencies()
	if httpLatencies.Splits[1] != 1 { // and so on
		t.Error("Wrong result")
	}
	httpLatencies = telemetryStorage.PopHTTPLatencies()
	if httpLatencies.Splits[1] != 0 { // and so on
		t.Error("Wrong result")
	}

	telemetryStorage.RecordAuthRejections()
	telemetryStorage.RecordAuthRejections()
	telemetryStorage.RecordTokenRefreshes()
	telemetryStorage.RecordAuthRejections()

	if telemetryStorage.PopAuthRejections() != 3 {
		t.Error("Wrong result")
	}
	if telemetryStorage.PopAuthRejections() != 0 {
		t.Error("Wrong result")
	}
	if telemetryStorage.PopTokenRefreshes() != 1 {
		t.Error("Wrong result")
	}
	if telemetryStorage.PopTokenRefreshes() != 0 {
		t.Error("Wrong result")
	}

	telemetryStorage.RecordStreamingEvent(dtos.StreamingEvent{Type: 1, Data: 1, Timestamp: 123456789})
	telemetryStorage.RecordStreamingEvent(dtos.StreamingEvent{Type: 1, Data: 1, Timestamp: 123456789})
	telemetryStorage.RecordStreamingEvent(dtos.StreamingEvent{Type: 1, Data: 1, Timestamp: 123456789})
	telemetryStorage.RecordStreamingEvent(dtos.StreamingEvent{Type: 1, Data: 1, Timestamp: 123456789})
	telemetryStorage.RecordStreamingEvent(dtos.StreamingEvent{Type: 1, Data: 1, Timestamp: 123456789})
	telemetryStorage.RecordStreamingEvent(dtos.StreamingEvent{Type: 1, Data: 1, Timestamp: 123456789})

	if len(telemetryStorage.PopStreamingEvents()) != 6 {
		t.Error("Wrong result")
	}
	if len(telemetryStorage.PopStreamingEvents()) != 0 {
		t.Error("Wrong result")
	}

	telemetryStorage.RecordSessionLength(123456789)
	if telemetryStorage.GetSessionLength() != 123456789 {
		t.Error("Wrong result")
	}

	telemetryStorage.AddTag("redo")
	telemetryStorage.AddTag("doc")
	tags := telemetryStorage.PopTags()
	if len(tags) != 2 {
		t.Error("Wrong result")
	}
	if len(telemetryStorage.PopTags()) != 0 {
		t.Error("Wrong result")
	}

	telemetryStorage.RecordBURTimeout()
	telemetryStorage.RecordBURTimeout()
	telemetryStorage.RecordBURTimeout()
	telemetryStorage.RecordNonReadyUsage()
	telemetryStorage.RecordNonReadyUsage()
	telemetryStorage.RecordNonReadyUsage()
	telemetryStorage.RecordNonReadyUsage()
	telemetryStorage.RecordNonReadyUsage()
	if telemetryStorage.GetBURTimeouts() != 3 {
		t.Error("Wrong result")
	}
	if telemetryStorage.GetNonReadyUsages() != 5 {
		t.Error("Wrong result")
	}
}
