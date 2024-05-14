package inmemory

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/telemetry"
)

func TestTelemetryStorage(t *testing.T) {
	telemetryStorage, _ := NewTelemetryStorage()

	telemetryStorage.RecordException(telemetry.Treatment)
	telemetryStorage.RecordException(telemetry.Treatments)
	telemetryStorage.RecordException(telemetry.Treatment)
	telemetryStorage.RecordException(telemetry.TreatmentsByFlagSet)
	telemetryStorage.RecordException(telemetry.TreatmentsByFlagSet)
	telemetryStorage.RecordException(telemetry.TreatmentsByFlagSets)
	telemetryStorage.RecordException(telemetry.TreatmentsByFlagSets)
	telemetryStorage.RecordException(telemetry.TreatmentsByFlagSets)
	telemetryStorage.RecordException(telemetry.TreatmentsWithConfigByFlagSet)
	telemetryStorage.RecordException(telemetry.TreatmentsWithConfigByFlagSets)
	telemetryStorage.RecordLatency(telemetry.Treatment, (1500 * time.Millisecond))
	telemetryStorage.RecordLatency(telemetry.Treatment, (2500 * time.Millisecond))
	telemetryStorage.RecordLatency(telemetry.Treatments, (18 * time.Millisecond))
	telemetryStorage.RecordLatency(telemetry.Treatments, (26 * time.Millisecond))
	telemetryStorage.RecordLatency(telemetry.TreatmentWithConfig, (800 * time.Millisecond))
	telemetryStorage.RecordLatency(telemetry.TreatmentsWithConfig, (1000 * time.Millisecond))
	telemetryStorage.RecordLatency(telemetry.TreatmentsByFlagSet, (130 * time.Millisecond))
	telemetryStorage.RecordLatency(telemetry.TreatmentsByFlagSets, (2600 * time.Millisecond))
	telemetryStorage.RecordLatency(telemetry.TreatmentsWithConfigByFlagSet, (650 * time.Millisecond))
	telemetryStorage.RecordLatency(telemetry.TreatmentsWithConfigByFlagSets, (10 * time.Millisecond))

	exceptions := telemetryStorage.PopExceptions()
	if exceptions.Treatment != 2 || exceptions.Treatments != 1 || exceptions.TreatmentWithConfig != 0 || exceptions.TreatmentsWithConfig != 0 || exceptions.TreatmentsByFlagSet != 2 || exceptions.TreatmentsByFlagSets != 3 || exceptions.TreatmentsWithConfigByFlagSet != 1 || exceptions.TreatmentsWithConfigByFlagSets != 1 || exceptions.Track != 0 {
		t.Error("Wrong result")
	}
	exceptions = telemetryStorage.PopExceptions()
	if exceptions.Treatment != 0 || exceptions.Treatments != 0 || exceptions.TreatmentWithConfig != 0 || exceptions.TreatmentsWithConfig != 0 || exceptions.Track != 0 {
		t.Error("Wrong result")
	}
	latencies := telemetryStorage.PopLatencies()
	if latencies.Treatment[19] != 1 || latencies.Treatment[20] != 1 {
		t.Error("Wrong result")
	}
	if latencies.Treatments[8] != 1 || latencies.Treatments[9] != 1 {
		t.Error("Wrong result")
	}
	if latencies.TreatmentWithConfig[17] != 1 {
		t.Error("Wrong result")
	}
	if latencies.TreatmentsWithConfig[18] != 1 {
		t.Error("Wrong result")
	}
	if latencies.TreatmentsByFlagSet[13] != 1 {
		t.Error("Wrong result")
	}
	if latencies.TreatmentsByFlagSets[20] != 1 {
		t.Error("Wrong result")
	}
	if latencies.TreatmentsWithConfigByFlagSet[16] != 1 {
		t.Error("Wrong result")
	}
	if latencies.TreatmentsWithConfigByFlagSets[6] != 1 {
		t.Error("Wrong result")
	}
	latencies = telemetryStorage.PopLatencies()
	if latencies.Treatment[1] != 0 {
		t.Error("Wrong result")
	}
	if latencies.TreatmentsByFlagSet[13] != 0 {
		t.Error("Wrong result")
	}
	if latencies.TreatmentsByFlagSets[20] != 0 {
		t.Error("Wrong result")
	}
	if latencies.TreatmentsWithConfigByFlagSet[16] != 0 {
		t.Error("Wrong result")
	}
	if latencies.TreatmentsWithConfigByFlagSets[6] != 0 {
		t.Error("Wrong result")
	}

	telemetryStorage.RecordImpressionsStats(telemetry.ImpressionsQueued, 200)
	telemetryStorage.RecordImpressionsStats(telemetry.ImpressionsDeduped, 100)
	telemetryStorage.RecordImpressionsStats(telemetry.ImpressionsDropped, 50)
	telemetryStorage.RecordImpressionsStats(telemetry.ImpressionsQueued, 200)
	if telemetryStorage.GetImpressionsStats(telemetry.ImpressionsDeduped) != 100 {
		t.Error("Wrong result")
	}
	if telemetryStorage.GetImpressionsStats(telemetry.ImpressionsQueued) != 400 {
		t.Error("Wrong result")
	}
	if telemetryStorage.GetImpressionsStats(telemetry.ImpressionsDropped) != 50 {
		t.Error("Wrong result")
	}

	telemetryStorage.RecordEventsStats(telemetry.EventsDropped, 100)
	telemetryStorage.RecordEventsStats(telemetry.EventsQueued, 10)
	telemetryStorage.RecordEventsStats(telemetry.EventsDropped, 100)
	telemetryStorage.RecordEventsStats(telemetry.EventsQueued, 10)
	if telemetryStorage.GetEventsStats(telemetry.EventsDropped) != 200 {
		t.Error("Wrong result")
	}
	if telemetryStorage.GetEventsStats(telemetry.EventsQueued) != 20 {
		t.Error("Wrong result")
	}

	telemetryStorage.RecordSuccessfulSync(telemetry.SplitSync, time.Now())
	time.Sleep(100 * time.Millisecond)
	telemetryStorage.RecordSuccessfulSync(telemetry.SegmentSync, time.Now())
	time.Sleep(100 * time.Millisecond)
	telemetryStorage.RecordSuccessfulSync(telemetry.ImpressionSync, time.Now())
	time.Sleep(100 * time.Millisecond)
	telemetryStorage.RecordSuccessfulSync(telemetry.ImpressionCountSync, time.Now())
	time.Sleep(100 * time.Millisecond)
	telemetryStorage.RecordSuccessfulSync(telemetry.EventSync, time.Now())
	time.Sleep(100 * time.Millisecond)
	telemetryStorage.RecordSuccessfulSync(telemetry.TelemetrySync, time.Now())
	time.Sleep(100 * time.Millisecond)
	telemetryStorage.RecordSuccessfulSync(telemetry.TokenSync, time.Now())
	lastSynchronization := telemetryStorage.GetLastSynchronization()
	if lastSynchronization.Splits == 0 || lastSynchronization.Segments == 0 || lastSynchronization.Impressions == 0 || lastSynchronization.ImpressionsCount == 0 || lastSynchronization.Events == 0 || lastSynchronization.Telemetry == 0 {
		t.Error("Wrong result")
	}

	telemetryStorage.RecordSyncError(telemetry.SplitSync, 500)
	telemetryStorage.RecordSyncError(telemetry.SplitSync, 500)
	telemetryStorage.RecordSyncError(telemetry.SplitSync, 500)
	telemetryStorage.RecordSyncError(telemetry.SplitSync, 500)
	telemetryStorage.RecordSyncError(telemetry.SplitSync, 500)
	telemetryStorage.RecordSyncError(telemetry.SegmentSync, 401)
	telemetryStorage.RecordSyncError(telemetry.SegmentSync, 401)
	telemetryStorage.RecordSyncError(telemetry.SegmentSync, 401)
	telemetryStorage.RecordSyncError(telemetry.SegmentSync, 404)
	telemetryStorage.RecordSyncError(telemetry.ImpressionSync, 402)
	telemetryStorage.RecordSyncError(telemetry.ImpressionSync, 402)
	telemetryStorage.RecordSyncError(telemetry.ImpressionSync, 402)
	telemetryStorage.RecordSyncError(telemetry.ImpressionSync, 402)
	telemetryStorage.RecordSyncError(telemetry.ImpressionCountSync, 404)
	telemetryStorage.RecordSyncError(telemetry.ImpressionCountSync, 404)
	telemetryStorage.RecordSyncError(telemetry.ImpressionCountSync, 404)
	telemetryStorage.RecordSyncError(telemetry.ImpressionCountSync, 402)
	telemetryStorage.RecordSyncError(telemetry.EventSync, 400)
	telemetryStorage.RecordSyncError(telemetry.TelemetrySync, 401)
	telemetryStorage.RecordSyncError(telemetry.TokenSync, 400)
	telemetryStorage.RecordSyncLatency(telemetry.SplitSync, (1500 * time.Millisecond))
	telemetryStorage.RecordSyncLatency(telemetry.SplitSync, (3000 * time.Millisecond))
	telemetryStorage.RecordSyncLatency(telemetry.SplitSync, (4000 * time.Millisecond))
	telemetryStorage.RecordSyncLatency(telemetry.SegmentSync, (1500 * time.Millisecond))
	telemetryStorage.RecordSyncLatency(telemetry.ImpressionCountSync, (1500 * time.Millisecond))
	telemetryStorage.RecordSyncLatency(telemetry.SegmentSync, (1500 * time.Millisecond))

	httpErrors := telemetryStorage.PopHTTPErrors()
	if httpErrors.Splits[500] != 5 || httpErrors.Segments[401] != 3 || httpErrors.Segments[404] != 1 || httpErrors.Impressions[402] != 4 || httpErrors.Events[400] != 1 || httpErrors.Telemetry[401] != 1 || httpErrors.Token[400] != 1 || httpErrors.ImpressionsCount[404] != 3 || httpErrors.ImpressionsCount[402] != 1 {
		t.Error("Wrong result")
	}
	httpErrors = telemetryStorage.PopHTTPErrors()
	if len(httpErrors.Splits) != 0 || len(httpErrors.Segments) != 0 {
		t.Error("Wrong result")
	}

	httpLatencies := telemetryStorage.PopHTTPLatencies()
	if httpLatencies.Splits[19] != 1 {
		t.Errorf("Wrong result: %+v", httpLatencies)
	}
	httpLatencies = telemetryStorage.PopHTTPLatencies()
	if httpLatencies.Splits[1] != 0 {
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

	telemetryStorage.RecordStreamingEvent(&dtos.StreamingEvent{Type: 1, Data: 1, Timestamp: 123456789})
	telemetryStorage.RecordStreamingEvent(&dtos.StreamingEvent{Type: 1, Data: 1, Timestamp: 123456789})
	telemetryStorage.RecordStreamingEvent(&dtos.StreamingEvent{Type: 1, Data: 1, Timestamp: 123456789})
	telemetryStorage.RecordStreamingEvent(&dtos.StreamingEvent{Type: 1, Data: 1, Timestamp: 123456789})
	telemetryStorage.RecordStreamingEvent(&dtos.StreamingEvent{Type: 1, Data: 1, Timestamp: 123456789})
	telemetryStorage.RecordStreamingEvent(&dtos.StreamingEvent{Type: 1, Data: 1, Timestamp: 123456789})

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

	telemetryStorage.RecordUpdatesFromSSE(telemetry.SplitUpdate)
	telemetryStorage.RecordUpdatesFromSSE(telemetry.SplitUpdate)
	telemetryStorage.RecordUpdatesFromSSE(telemetry.SplitUpdate)
	updatesFromSSE := telemetryStorage.PopUpdatesFromSSE()
	if updatesFromSSE.Splits != 3 {
		t.Error("It should track 3")
	}
	if telemetryStorage.counters.splitUpdates != 0 {
		t.Error("It should be reset")
	}
	updatesFromSSE = telemetryStorage.PopUpdatesFromSSE()
	if updatesFromSSE.Splits != 0 {
		t.Error("It should track 0")
	}
}
