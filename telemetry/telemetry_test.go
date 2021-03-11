package telemetry

import (
	"fmt"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v3/constants"
	"github.com/splitio/go-split-commons/v3/dtos"
	"github.com/splitio/go-split-commons/v3/storage/inmemory"
	"github.com/splitio/go-split-commons/v3/storage/inmemory/mutexmap"
	"github.com/splitio/go-split-commons/v3/storage/mocks"
	"github.com/splitio/go-toolkit/v4/datastructures/set"
)

func TestEvaluationTelemetry(t *testing.T) {
	exceptionCalled := 0
	popExceptionCalled := 0
	latencyCalled := 0
	popLatencyCalled := 0
	mockedTelemetryStorage := mocks.MockTelemetryStorage{
		RecordExceptionCall: func(method int) {
			switch exceptionCalled {
			case 0:
				if method != constants.Treatment {
					t.Error("Unexpected method call")
				}
			case 1:
				if method != constants.Treatments {
					t.Error("Unexpected method call")
				}
			case 2:
				if method != constants.TreatmentWithConfig {
					t.Error("Unexpected method call")
				}
			case 3:
				if method != constants.TreatmentsWithConfig {
					t.Error("Unexpected method call")
				}
			case 4:
				if method != constants.Track {
					t.Error("Unexpected method call")
				}
			}
			exceptionCalled++
		},
		PopExceptionsCall: func() dtos.MethodExceptions {
			popExceptionCalled++
			return dtos.MethodExceptions{}
		},
		RecordLatencyCall: func(method, bucket int) {
			switch latencyCalled {
			case 0:
				if method != constants.Treatment || bucket != 0 {
					t.Error("Unexpected method call")
				}
			case 1:
				if method != constants.Treatments || bucket != 1 {
					t.Error("Unexpected method call")
				}
			case 2:
				if method != constants.TreatmentWithConfig || bucket != 4 {
					t.Error("Unexpected method call")
				}
			case 3:
				if method != constants.TreatmentsWithConfig || bucket != 5 {
					t.Error("Unexpected method call")
				}
			case 4:
				if method != constants.Track || bucket != 6 {
					t.Error("Unexpected method call")
				}
			}
			latencyCalled++
		},
		PopLatenciesCall: func() dtos.MethodLatencies {
			popLatencyCalled++
			return dtos.MethodLatencies{}
		},
	}
	mockedSplitStorage := mocks.MockSplitStorage{}
	mockedSegmentStorage := mocks.MockSegmentStorage{}
	telemetryService := NewTelemetry(mockedTelemetryStorage, mockedSplitStorage, mockedSegmentStorage)

	telemetryService.RecordException(constants.Treatment)
	telemetryService.RecordException(constants.Treatments)
	telemetryService.RecordException(constants.TreatmentWithConfig)
	telemetryService.RecordException(constants.TreatmentsWithConfig)
	telemetryService.RecordException(constants.Track)
	if exceptionCalled != 5 {
		t.Error("It should be called 5 times")
	}

	telemetryService.PopExceptions()
	if popExceptionCalled != 1 {
		t.Error("It should be called once")
	}

	telemetryService.RecordLatency(constants.Treatment, (500 * time.Nanosecond).Nanoseconds())
	telemetryService.RecordLatency(constants.Treatments, (1500 * time.Nanosecond).Nanoseconds())
	telemetryService.RecordLatency(constants.TreatmentWithConfig, (3500 * time.Nanosecond).Nanoseconds())
	telemetryService.RecordLatency(constants.TreatmentsWithConfig, (5500 * time.Nanosecond).Nanoseconds())
	telemetryService.RecordLatency(constants.Track, (8000 * time.Nanosecond).Nanoseconds())
	if latencyCalled != 5 {
		t.Error("It should be called 5 times")
	}

	telemetryService.PopLatencies()
	if popLatencyCalled != 1 {
		t.Error("It should be called once")
	}
}

func TestImpressionsTelemetry(t *testing.T) {
	called := 0
	mockedSplitStorage := mocks.MockSplitStorage{}
	mockedSegmentStorage := mocks.MockSegmentStorage{}
	mockedTelemetryStorage := mocks.MockTelemetryStorage{
		RecordImpressionsStatsCall: func(dataType int, count int64) {
			switch called {
			case 0:
				if dataType != constants.ImpressionsDropped || count != 100 {
					t.Error("Wrong type")
				}
			case 1:
				if dataType != constants.ImpressionsDeduped || count != 200 {
					t.Error("Wrong type")
				}
			case 2:
				if dataType != constants.ImpressionsQueued || count != 300 {
					t.Error("Wrong type")
				}
			}
			called++
		},
		GetImpressionsStatsCall: func(dataType int) int64 {
			switch dataType {
			case constants.ImpressionsDropped:
				return 100
			case constants.ImpressionsDeduped:
				return 200
			case constants.ImpressionsQueued:
				return 300
			}
			return 0
		},
	}
	telemetryService := NewTelemetry(mockedTelemetryStorage, mockedSplitStorage, mockedSegmentStorage)

	telemetryService.RecordImpressionsStats(constants.ImpressionsDropped, 100)
	telemetryService.RecordImpressionsStats(constants.ImpressionsDeduped, 200)
	telemetryService.RecordImpressionsStats(constants.ImpressionsQueued, 300)
	if called != 3 {
		t.Error("It should be called three times")
	}

	if telemetryService.GetImpressionsStats(constants.ImpressionsDropped) != 100 {
		t.Error("Unexpected result")
	}
	if telemetryService.GetImpressionsStats(constants.ImpressionsDeduped) != 200 {
		t.Error("Unexpected result")
	}
	if telemetryService.GetImpressionsStats(constants.ImpressionsQueued) != 300 {
		t.Error("Unexpected result")
	}
}

func TestEventsTelemetry(t *testing.T) {
	called := 0
	mockedSplitStorage := mocks.MockSplitStorage{}
	mockedSegmentStorage := mocks.MockSegmentStorage{}
	mockedTelemetryStorage := mocks.MockTelemetryStorage{
		RecordEventsStatsCall: func(dataType int, count int64) {
			switch called {
			case 0:
				if dataType != constants.EventsDropped || count != 100 {
					t.Error("Wrong type")
				}
			case 1:
				if dataType != constants.EventsQueued || count != 200 {
					t.Error("Wrong type")
				}
			}
			called++
		},
		GetEventsStatsCall: func(dataType int) int64 {
			switch dataType {
			case constants.EventsDropped:
				return 100
			case constants.EventsQueued:
				return 200
			}
			return 0
		},
	}
	telemetryService := NewTelemetry(mockedTelemetryStorage, mockedSplitStorage, mockedSegmentStorage)

	telemetryService.RecordEventsStats(constants.EventsDropped, 100)
	telemetryService.RecordEventsStats(constants.EventsQueued, 200)
	if called != 2 {
		t.Error("It should be called twice")
	}

	if telemetryService.GetEventsStats(constants.EventsDropped) != 100 {
		t.Error("Unexpected result")
	}
	if telemetryService.GetEventsStats(constants.EventsQueued) != 200 {
		t.Error("Unexpected result")
	}
}

func TestSynchronizationTelemetry(t *testing.T) {
	called := 0
	getCalled := 0
	mockedSplitStorage := mocks.MockSplitStorage{}
	mockedSegmentStorage := mocks.MockSegmentStorage{}
	mockedTelemetryStorage := mocks.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, tm int64) {
			passed := time.Unix(0, tm*int64(time.Millisecond))
			if time.Now().Before(passed) {
				t.Error("Wrong time")
			}
			switch called {
			case 0:
				if resource != constants.SplitSync {
					t.Error("Wrong call")
				}
			case 1:
				if resource != constants.SegmentSync {
					t.Error("Wrong call")
				}
			case 2:
				if resource != constants.ImpressionSync {
					t.Error("Wrong call")
				}
			case 3:
				if resource != constants.EventSync {
					t.Error("Wrong call")
				}
			case 4:
				if resource != constants.TelemetrySync {
					t.Error("Wrong call")
				}
			case 5:
				if resource != constants.TokenSync {
					t.Error("Wrong call")
				}
			}
			called++
		},
		GetLastSynchronizationCall: func() dtos.LastSynchronization {
			getCalled++
			return dtos.LastSynchronization{}
		},
	}
	telemetryService := NewTelemetry(mockedTelemetryStorage, mockedSplitStorage, mockedSegmentStorage)

	telemetryService.RecordSuccessfulSync(constants.SplitSync)
	telemetryService.RecordSuccessfulSync(constants.SegmentSync)
	telemetryService.RecordSuccessfulSync(constants.ImpressionSync)
	telemetryService.RecordSuccessfulSync(constants.EventSync)
	telemetryService.RecordSuccessfulSync(constants.TelemetrySync)
	telemetryService.RecordSuccessfulSync(constants.TokenSync)
	if called != 6 {
		t.Error("It should be called six times")
	}

	telemetryService.GetLastSynchronization()
	if getCalled != 1 {
		t.Error("It should be called once")
	}
}

func TestHTTPTelemetry(t *testing.T) {
	called := 0
	getCalled := 0
	latencyCalled := 0
	popLatencyCalled := 0
	mockedSplitStorage := mocks.MockSplitStorage{}
	mockedSegmentStorage := mocks.MockSegmentStorage{}
	mockedTelemetryStorage := mocks.MockTelemetryStorage{
		RecordSyncErrorCall: func(resource, status int) {
			switch called {
			case 0:
				if resource != constants.SplitSync || status != 500 {
					t.Error("Wrong call")
				}
			case 1:
				if resource != constants.SegmentSync || status != 401 {
					t.Error("Wrong call")
				}
			case 2:
				if resource != constants.ImpressionSync || status != 402 {
					t.Error("Wrong call")
				}
			case 3:
				if resource != constants.EventSync || status != 400 {
					t.Error("Wrong call")
				}
			case 4:
				if resource != constants.TelemetrySync || status != 404 {
					t.Error("Wrong call")
				}
			case 5:
				if resource != constants.TokenSync || status != 403 {
					t.Error("Wrong call")
				}
			}
			called++
		},
		PopHTTPErrorsCall: func() dtos.HTTPErrors {
			getCalled++
			return dtos.HTTPErrors{}
		},
		RecordSyncLatencyCall: func(resource, bucket int) {
			switch latencyCalled {
			case 0:
				if resource != constants.SplitSync || bucket != 0 {
					t.Error("Unexpected method call")
				}
			case 1:
				if resource != constants.SegmentSync || bucket != 1 {
					t.Error("Unexpected method call")
				}
			case 2:
				if resource != constants.ImpressionSync || bucket != 4 {
					t.Error("Unexpected method call")
				}
			case 3:
				if resource != constants.EventSync || bucket != 5 {
					t.Error("Unexpected method call")
				}
			case 4:
				if resource != constants.TelemetrySync || bucket != 6 {
					t.Error("Unexpected method call")
				}
			case 5:
				if resource != constants.TokenSync || bucket != 8 {
					t.Error("Unexpected method call")
				}
			}
			latencyCalled++
		},
		PopHTTPLatenciesCall: func() dtos.HTTPLatencies {
			popLatencyCalled++
			return dtos.HTTPLatencies{}
		},
	}
	telemetryService := NewTelemetry(mockedTelemetryStorage, mockedSplitStorage, mockedSegmentStorage)

	telemetryService.RecordSyncError(constants.SplitSync, 500)
	telemetryService.RecordSyncError(constants.SegmentSync, 401)
	telemetryService.RecordSyncError(constants.ImpressionSync, 402)
	telemetryService.RecordSyncError(constants.EventSync, 400)
	telemetryService.RecordSyncError(constants.TelemetrySync, 404)
	telemetryService.RecordSyncError(constants.TokenSync, 403)
	if called != 6 {
		t.Error("It should be called six times")
	}

	telemetryService.PopHTTPErrors()
	if getCalled != 1 {
		t.Error("It should be called once")
	}

	telemetryService.RecordSyncLatency(constants.SplitSync, (500 * time.Nanosecond).Nanoseconds())
	telemetryService.RecordSyncLatency(constants.SegmentSync, (1500 * time.Nanosecond).Nanoseconds())
	telemetryService.RecordSyncLatency(constants.ImpressionSync, (3500 * time.Nanosecond).Nanoseconds())
	telemetryService.RecordSyncLatency(constants.EventSync, (5500 * time.Nanosecond).Nanoseconds())
	telemetryService.RecordSyncLatency(constants.TelemetrySync, (8000 * time.Nanosecond).Nanoseconds())
	telemetryService.RecordSyncLatency(constants.TokenSync, (20000 * time.Nanosecond).Nanoseconds())
	if latencyCalled != 6 {
		t.Error("It should be called six times")
	}

	telemetryService.PopHTTPLatencies()
	if popLatencyCalled != 1 {
		t.Error("It should be called once")
	}
}

func TestCacheTelemetry(t *testing.T) {
	mockedSplitStorage := mocks.MockSplitStorage{
		SplitNamesCall: func() []string {
			return []string{"split1", "split2"}
		},
		SegmentNamesCall: func() *set.ThreadUnsafeSet {
			return set.NewSet("segment1", "segment2", "segment3")
		},
	}
	mockedSegmentStorage := mocks.MockSegmentStorage{
		KeysCall: func(segmentName string) *set.ThreadUnsafeSet {
			switch segmentName {
			case "segment1":
				return set.NewSet("k1")
			case "segment2":
				return set.NewSet("k2", "k3", "k4")
			case "segment3":
				return set.NewSet()
			}
			return set.NewSet()
		},
	}
	mockedTelemetryStorage := mocks.MockTelemetryStorage{}
	telemetryService := NewTelemetry(mockedTelemetryStorage, mockedSplitStorage, mockedSegmentStorage)

	if telemetryService.GetSplitsCount() != 2 {
		t.Error("It should be 2")
	}

	if telemetryService.GetSegmentsCount() != 3 {
		t.Error("It should be 3")
	}

	if telemetryService.GetSegmentKeysCount() != 4 {
		t.Error("It should be 3")
	}
}

func TestPushTelemetry(t *testing.T) {
	called := 0
	tokenCalled := 0
	mockedSplitStorage := mocks.MockSplitStorage{}
	mockedSegmentStorage := mocks.MockSegmentStorage{}
	mockedTelemetryStorage := mocks.MockTelemetryStorage{
		RecordAuthRejectionsCall: func() {
			called++
		},
		PopAuthRejectionsCall: func() int64 {
			return 100
		},
		RecordTokenRefreshesCall: func() {
			tokenCalled++
		},
		PopTokenRefreshesCall: func() int64 {
			return 200
		},
	}
	telemetryService := NewTelemetry(mockedTelemetryStorage, mockedSplitStorage, mockedSegmentStorage)

	telemetryService.RecordAuthRejections()
	if called != 1 {
		t.Error("It should be called once")
	}

	if telemetryService.PopAuthRejections() != 100 {
		t.Error("Unexpected result")
	}

	telemetryService.RecordTokenRefreshes()
	if tokenCalled != 1 {
		t.Error("It should be called once")
	}

	if telemetryService.PopTokenRefreshes() != 200 {
		t.Error("Unexpected result")
	}
}

func TestStreamingTelemetry(t *testing.T) {
	called := 0
	mockedSplitStorage := mocks.MockSplitStorage{}
	mockedSegmentStorage := mocks.MockSegmentStorage{}
	mockedTelemetryStorage := mocks.MockTelemetryStorage{
		RecordStreamingEventCall: func(streamingEvent dtos.StreamingEvent) {
			switch called {
			case 0:
				if streamingEvent.Type != constants.EventTypeSSEConnectionEstablished || streamingEvent.Data != 0 {
					t.Error("Unexpected streamingEvent")
				}
			case 1:
				if streamingEvent.Type != constants.EventTypeOccupancyPri || streamingEvent.Data != 10 {
					t.Error("Unexpected streamingEvent")
				}
			case 2:
				if streamingEvent.Type != constants.EventTypeOccupancySec || streamingEvent.Data != 1 {
					t.Error("Unexpected streamingEvent")
				}
			case 3:
				if streamingEvent.Type != constants.EventTypeStreamingStatus || streamingEvent.Data != constants.StreamingDisabled {
					t.Error("Unexpected streamingEvent")
				}
			case 4:
				if streamingEvent.Type != constants.EventTypeConnectionError || streamingEvent.Data != constants.NonRequested {
					t.Error("Unexpected streamingEvent")
				}
			case 5:
				if streamingEvent.Type != constants.EventTypeTokenRefresh || streamingEvent.Data != 123456789 {
					t.Error("Unexpected streamingEvent")
				}
			case 6:
				if streamingEvent.Type != constants.EventTypeAblyError || streamingEvent.Data != 40010 {
					t.Error("Unexpected streamingEvent")
				}
			case 7:
				if streamingEvent.Type != constants.EventTypeSyncMode || streamingEvent.Data != constants.Polling {
					t.Error("Unexpected streamingEvent")
				}
			}
			called++
		},
		PopStreamingEventsCall: func() []dtos.StreamingEvent {
			return []dtos.StreamingEvent{
				{Type: 0, Data: 1, Timestamp: 1234567},
				{Type: 1, Data: 2, Timestamp: 1234567},
			}
		},
	}
	telemetryService := NewTelemetry(mockedTelemetryStorage, mockedSplitStorage, mockedSegmentStorage)

	telemetryService.RecordStreamingEvent(constants.EventTypeSSEConnectionEstablished, 0)
	telemetryService.RecordStreamingEvent(constants.EventTypeOccupancyPri, 10)
	telemetryService.RecordStreamingEvent(constants.EventTypeOccupancySec, 1)
	telemetryService.RecordStreamingEvent(constants.EventTypeStreamingStatus, constants.StreamingDisabled)
	telemetryService.RecordStreamingEvent(constants.EventTypeConnectionError, constants.NonRequested)
	telemetryService.RecordStreamingEvent(constants.EventTypeTokenRefresh, 123456789)
	telemetryService.RecordStreamingEvent(constants.EventTypeAblyError, 40010)
	telemetryService.RecordStreamingEvent(constants.EventTypeSyncMode, constants.Polling)
	if called != 8 {
		t.Error("It should be called eight times")
	}

	if len(telemetryService.PopStreamingEvents()) != 2 {
		t.Error("Unexpected result")
	}
}

func TestMiscTelemetry(t *testing.T) {
	called := 0
	mockedSplitStorage := mocks.MockSplitStorage{}
	mockedSegmentStorage := mocks.MockSegmentStorage{}
	mockedTelemetryStorage := mocks.MockTelemetryStorage{
		AddTagCall: func(tag string) {
			if tag != "test1" {
				t.Error("It should be test1")
			}
			called++
		},
		PopTagsCall: func() []string {
			return []string{"one", "two"}
		},
	}
	telemetryService := NewTelemetry(mockedTelemetryStorage, mockedSplitStorage, mockedSegmentStorage)

	telemetryService.AddTag("test1")
	if called != 1 {
		t.Error("It should be called once")
	}

	if len(telemetryService.PopTags()) != 2 {
		t.Error("It should be 2")
	}
}

func TestSDKInfoTelemetry(t *testing.T) {
	called := 0
	mockedSplitStorage := mocks.MockSplitStorage{}
	mockedSegmentStorage := mocks.MockSegmentStorage{}
	mockedTelemetryStorage := mocks.MockTelemetryStorage{
		RecordSessionLengthCall: func(session int64) {
			if session != 123456789 {
				t.Error("Unexpected session passed")
			}
			called++
		},
		GetSessionLengthCall: func() int64 {
			return 123456789
		},
	}
	telemetryService := NewTelemetry(mockedTelemetryStorage, mockedSplitStorage, mockedSegmentStorage)

	telemetryService.RecordSessionLength(123456789)
	if called != 1 {
		t.Error("It should be called once")
	}

	if telemetryService.GetSessionLength() != 123456789 {
		t.Error("Unexpected result")
	}
}

func TestFactoryTelemetry(t *testing.T) {
	called := 0
	burCalled := 0
	mockedSplitStorage := mocks.MockSplitStorage{}
	mockedSegmentStorage := mocks.MockSegmentStorage{}
	mockedTelemetryStorage := mocks.MockTelemetryStorage{
		RecordNonReadyUsageCall: func() {
			called++
		},
		GetNonReadyUsagesCall: func() int64 {
			return 30
		},
		RecordBURTimeoutCall: func() {
			burCalled++
		},
		GetBURTimeoutsCall: func() int64 {
			return 50
		},
	}
	telemetryService := NewTelemetry(mockedTelemetryStorage, mockedSplitStorage, mockedSegmentStorage)

	telemetryService.RecordNonReadyUsage()
	if called != 1 {
		t.Error("It should be called once")
	}

	if telemetryService.GetNonReadyUsages() != 30 {
		t.Error("Unexpected result")
	}

	telemetryService.RecordBURTimeout()
	if burCalled != 1 {
		t.Error("It should be called once")
	}

	if telemetryService.GetBURTimeouts() != 50 {
		t.Error("Unexpected result")
	}
}

func TestTelemetryService(t *testing.T) {
	splitStorage := mutexmap.NewMMSplitStorage()
	splits := make([]dtos.SplitDTO, 0, 10)
	for index := 0; index < 10; index++ {
		splits = append(splits, dtos.SplitDTO{
			Name: fmt.Sprintf("SomeSplit_%d", index),
			Algo: index,
		})
	}
	splitStorage.PutMany(splits, 123)
	segmentStorage := mutexmap.NewMMSegmentStorage()
	segmentStorage.Update("some", set.NewSet("doc", "redo"), set.NewSet(), 123456789)

	telemetryService := NewTelemetry(inmemory.NewIMTelemetryStorage(), splitStorage, segmentStorage)

	telemetryService.RecordException(constants.Treatment)
	telemetryService.RecordException(constants.Treatments)
	telemetryService.RecordException(constants.Treatment)
	telemetryService.RecordLatency(constants.Treatment, (1500 * time.Nanosecond).Nanoseconds())
	telemetryService.RecordLatency(constants.Treatment, (2000 * time.Nanosecond).Nanoseconds())
	telemetryService.RecordLatency(constants.Treatments, (3000 * time.Nanosecond).Nanoseconds())
	telemetryService.RecordLatency(constants.Treatments, (500 * time.Nanosecond).Nanoseconds())
	telemetryService.RecordLatency(constants.TreatmentWithConfig, (800 * time.Nanosecond).Nanoseconds())
	telemetryService.RecordLatency(constants.TreatmentsWithConfig, (1000 * time.Nanosecond).Nanoseconds())

	exceptions := telemetryService.PopExceptions()
	if exceptions.Treatment != 2 || exceptions.Treatments != 1 || exceptions.TreatmentWithConfig != 0 || exceptions.TreatmentWithConfigs != 0 || exceptions.Track != 0 {
		t.Error("Wrong result")
	}
	exceptions = telemetryService.PopExceptions()
	if exceptions.Treatment != 0 || exceptions.Treatments != 0 || exceptions.TreatmentWithConfig != 0 || exceptions.TreatmentWithConfigs != 0 || exceptions.Track != 0 {
		t.Error("Wrong result")
	}
	latencies := telemetryService.PopLatencies()
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
	latencies = telemetryService.PopLatencies()
	if latencies.Treatment[1] != 0 {
		t.Error("Wrong result")
	}

	telemetryService.RecordImpressionsStats(constants.ImpressionsQueued, 200)
	telemetryService.RecordImpressionsStats(constants.ImpressionsDeduped, 100)
	telemetryService.RecordImpressionsStats(constants.ImpressionsDropped, 50)
	telemetryService.RecordImpressionsStats(constants.ImpressionsQueued, 200)
	if telemetryService.GetImpressionsStats(constants.ImpressionsDeduped) != 100 {
		t.Error("Wrong result")
	}
	if telemetryService.GetImpressionsStats(constants.ImpressionsQueued) != 400 {
		t.Error("Wrong result")
	}
	if telemetryService.GetImpressionsStats(constants.ImpressionsDropped) != 50 {
		t.Error("Wrong result")
	}

	telemetryService.RecordEventsStats(constants.EventsDropped, 100)
	telemetryService.RecordEventsStats(constants.EventsQueued, 10)
	telemetryService.RecordEventsStats(constants.EventsDropped, 100)
	telemetryService.RecordEventsStats(constants.EventsQueued, 10)
	if telemetryService.GetEventsStats(constants.EventsDropped) != 200 {
		t.Error("Wrong result")
	}
	if telemetryService.GetEventsStats(constants.EventsQueued) != 20 {
		t.Error("Wrong result")
	}

	telemetryService.RecordSuccessfulSync(constants.SplitSync)
	time.Sleep(100 * time.Millisecond)
	telemetryService.RecordSuccessfulSync(constants.SegmentSync)
	time.Sleep(100 * time.Millisecond)
	telemetryService.RecordSuccessfulSync(constants.ImpressionSync)
	time.Sleep(100 * time.Millisecond)
	telemetryService.RecordSuccessfulSync(constants.EventSync)
	time.Sleep(100 * time.Millisecond)
	telemetryService.RecordSuccessfulSync(constants.TelemetrySync)
	time.Sleep(100 * time.Millisecond)
	telemetryService.RecordSuccessfulSync(constants.TokenSync)

	lastSynchronization := telemetryService.GetLastSynchronization()
	if lastSynchronization.Splits == 0 || lastSynchronization.Segments == 0 || lastSynchronization.Impressions == 0 || lastSynchronization.Events == 0 || lastSynchronization.Telemetry == 0 {
		t.Error("Wrong result")
	}

	telemetryService.RecordSyncError(constants.SplitSync, 500)
	telemetryService.RecordSyncError(constants.SplitSync, 500)
	telemetryService.RecordSyncError(constants.SplitSync, 500)
	telemetryService.RecordSyncError(constants.SplitSync, 500)
	telemetryService.RecordSyncError(constants.SplitSync, 500)
	telemetryService.RecordSyncError(constants.SegmentSync, 401)
	telemetryService.RecordSyncError(constants.SegmentSync, 401)
	telemetryService.RecordSyncError(constants.SegmentSync, 401)
	telemetryService.RecordSyncError(constants.SegmentSync, 404)
	telemetryService.RecordSyncError(constants.ImpressionSync, 402)
	telemetryService.RecordSyncError(constants.ImpressionSync, 402)
	telemetryService.RecordSyncError(constants.ImpressionSync, 402)
	telemetryService.RecordSyncError(constants.ImpressionSync, 402)
	telemetryService.RecordSyncError(constants.EventSync, 400)
	telemetryService.RecordSyncError(constants.TelemetrySync, 401)
	telemetryService.RecordSyncError(constants.TokenSync, 400)
	telemetryService.RecordSyncLatency(constants.SplitSync, (1500 * time.Nanosecond).Nanoseconds())
	telemetryService.RecordSyncLatency(constants.SplitSync, (3000 * time.Nanosecond).Nanoseconds())
	telemetryService.RecordSyncLatency(constants.SplitSync, (4000 * time.Nanosecond).Nanoseconds())
	telemetryService.RecordSyncLatency(constants.SegmentSync, (1500 * time.Nanosecond).Nanoseconds())
	telemetryService.RecordSyncLatency(constants.SegmentSync, (1500 * time.Nanosecond).Nanoseconds())

	httpErrors := telemetryService.PopHTTPErrors()
	if httpErrors.Splits[500] != 5 || httpErrors.Segments[401] != 3 || httpErrors.Segments[404] != 1 || httpErrors.Impressions[402] != 4 || httpErrors.Events[400] != 1 || httpErrors.Telemetry[401] != 1 || httpErrors.Token[400] != 1 {
		t.Error("Wrong result")
	}
	httpErrors = telemetryService.PopHTTPErrors()
	if len(httpErrors.Splits) != 0 || len(httpErrors.Segments) != 0 { // and so on
		t.Error("Wrong result")
	}

	httpLatencies := telemetryService.PopHTTPLatencies()
	if httpLatencies.Splits[1] != 1 { // and so on
		t.Error("Wrong result")
	}
	httpLatencies = telemetryService.PopHTTPLatencies()
	if httpLatencies.Splits[1] != 0 { // and so on
		t.Error("Wrong result")
	}

	if telemetryService.GetSplitsCount() != 10 {
		t.Error("Wrong result")
	}

	telemetryService.RecordAuthRejections()
	telemetryService.RecordAuthRejections()
	telemetryService.RecordTokenRefreshes()
	telemetryService.RecordAuthRejections()

	if telemetryService.PopAuthRejections() != 3 {
		t.Error("Wrong result")
	}
	if telemetryService.PopAuthRejections() != 0 {
		t.Error("Wrong result")
	}
	if telemetryService.PopTokenRefreshes() != 1 {
		t.Error("Wrong result")
	}
	if telemetryService.PopTokenRefreshes() != 0 {
		t.Error("Wrong result")
	}

	telemetryService.RecordStreamingEvent(constants.EventTypeAblyError, 40010)
	telemetryService.RecordStreamingEvent(constants.EventTypeSSEConnectionEstablished, 0)
	telemetryService.RecordStreamingEvent(constants.EventTypeOccupancyPri, 10)
	telemetryService.RecordStreamingEvent(constants.EventTypeOccupancySec, 1)
	telemetryService.RecordStreamingEvent(constants.EventTypeSyncMode, constants.Polling)

	if len(telemetryService.PopStreamingEvents()) != 5 {
		t.Error("Wrong result")
	}
	if len(telemetryService.PopStreamingEvents()) != 0 {
		t.Error("Wrong result")
	}

	telemetryService.RecordSessionLength(123456789)
	if telemetryService.GetSessionLength() != 123456789 {
		t.Error("Wrong result")
	}

	telemetryService.AddTag("redo")
	telemetryService.AddTag("doc")
	tags := telemetryService.PopTags()
	if len(tags) != 2 {
		t.Error("Wrong result")
	}
	if len(telemetryService.PopTags()) != 0 {
		t.Error("Wrong result")
	}

	telemetryService.RecordBURTimeout()
	telemetryService.RecordBURTimeout()
	telemetryService.RecordBURTimeout()
	telemetryService.RecordNonReadyUsage()
	telemetryService.RecordNonReadyUsage()
	telemetryService.RecordNonReadyUsage()
	telemetryService.RecordNonReadyUsage()
	telemetryService.RecordNonReadyUsage()
	if telemetryService.GetBURTimeouts() != 3 {
		t.Error("Wrong result")
	}
	if telemetryService.GetNonReadyUsages() != 5 {
		t.Error("Wrong result")
	}
}
