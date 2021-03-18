package telemetry

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v3/dtos"
	"github.com/splitio/go-split-commons/v3/storage/mocks"
	"github.com/splitio/go-toolkit/v4/datastructures/set"
)

func TestEvaluationTelemetry(t *testing.T) {
	exceptionCalled := 0
	popExceptionCalled := 0
	latencyCalled := 0
	popLatencyCalled := 0
	mockedTelemetryStorage := mocks.MockTelemetryStorage{
		RecordExceptionCall: func(method string) {
			switch exceptionCalled {
			case 0:
				if method != Treatment {
					t.Error("Unexpected method call")
				}
			case 1:
				if method != Treatments {
					t.Error("Unexpected method call")
				}
			case 2:
				if method != TreatmentWithConfig {
					t.Error("Unexpected method call")
				}
			case 3:
				if method != TreatmentsWithConfig {
					t.Error("Unexpected method call")
				}
			case 4:
				if method != Track {
					t.Error("Unexpected method call")
				}
			}
			exceptionCalled++
		},
		PopExceptionsCall: func() dtos.MethodExceptions {
			popExceptionCalled++
			return dtos.MethodExceptions{}
		},
		RecordLatencyCall: func(method string, bucket int) {
			switch latencyCalled {
			case 0:
				if method != Treatment || bucket != 0 {
					t.Error("Unexpected method call")
				}
			case 1:
				if method != Treatments || bucket != 1 {
					t.Error("Unexpected method call")
				}
			case 2:
				if method != TreatmentWithConfig || bucket != 4 {
					t.Error("Unexpected method call")
				}
			case 3:
				if method != TreatmentsWithConfig || bucket != 5 {
					t.Error("Unexpected method call")
				}
			case 4:
				if method != Track || bucket != 6 {
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

	telemetryService.RecordException(Treatment)
	telemetryService.RecordException(Treatments)
	telemetryService.RecordException(TreatmentWithConfig)
	telemetryService.RecordException(TreatmentsWithConfig)
	telemetryService.RecordException(Track)
	if exceptionCalled != 5 {
		t.Error("It should be called 5 times")
	}

	telemetryService.PopExceptions()
	if popExceptionCalled != 1 {
		t.Error("It should be called once")
	}

	telemetryService.RecordLatency(Treatment, (500 * time.Nanosecond).Nanoseconds())
	telemetryService.RecordLatency(Treatments, (1500 * time.Nanosecond).Nanoseconds())
	telemetryService.RecordLatency(TreatmentWithConfig, (3500 * time.Nanosecond).Nanoseconds())
	telemetryService.RecordLatency(TreatmentsWithConfig, (5500 * time.Nanosecond).Nanoseconds())
	telemetryService.RecordLatency(Track, (8000 * time.Nanosecond).Nanoseconds())
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
				if dataType != ImpressionsDropped || count != 100 {
					t.Error("Wrong type")
				}
			case 1:
				if dataType != ImpressionsDeduped || count != 200 {
					t.Error("Wrong type")
				}
			case 2:
				if dataType != ImpressionsQueued || count != 300 {
					t.Error("Wrong type")
				}
			}
			called++
		},
		GetImpressionsStatsCall: func(dataType int) int64 {
			switch dataType {
			case ImpressionsDropped:
				return 100
			case ImpressionsDeduped:
				return 200
			case ImpressionsQueued:
				return 300
			}
			return 0
		},
	}
	telemetryService := NewTelemetry(mockedTelemetryStorage, mockedSplitStorage, mockedSegmentStorage)

	telemetryService.RecordImpressionsStats(ImpressionsDropped, 100)
	telemetryService.RecordImpressionsStats(ImpressionsDeduped, 200)
	telemetryService.RecordImpressionsStats(ImpressionsQueued, 300)
	if called != 3 {
		t.Error("It should be called three times")
	}

	if telemetryService.GetImpressionsStats(ImpressionsDropped) != 100 {
		t.Error("Unexpected result")
	}
	if telemetryService.GetImpressionsStats(ImpressionsDeduped) != 200 {
		t.Error("Unexpected result")
	}
	if telemetryService.GetImpressionsStats(ImpressionsQueued) != 300 {
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
				if dataType != EventsDropped || count != 100 {
					t.Error("Wrong type")
				}
			case 1:
				if dataType != EventsQueued || count != 200 {
					t.Error("Wrong type")
				}
			}
			called++
		},
		GetEventsStatsCall: func(dataType int) int64 {
			switch dataType {
			case EventsDropped:
				return 100
			case EventsQueued:
				return 200
			}
			return 0
		},
	}
	telemetryService := NewTelemetry(mockedTelemetryStorage, mockedSplitStorage, mockedSegmentStorage)

	telemetryService.RecordEventsStats(EventsDropped, 100)
	telemetryService.RecordEventsStats(EventsQueued, 200)
	if called != 2 {
		t.Error("It should be called twice")
	}

	if telemetryService.GetEventsStats(EventsDropped) != 100 {
		t.Error("Unexpected result")
	}
	if telemetryService.GetEventsStats(EventsQueued) != 200 {
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
				if resource != SplitSync {
					t.Error("Wrong call")
				}
			case 1:
				if resource != SegmentSync {
					t.Error("Wrong call")
				}
			case 2:
				if resource != ImpressionSync {
					t.Error("Wrong call")
				}
			case 3:
				if resource != EventSync {
					t.Error("Wrong call")
				}
			case 4:
				if resource != TelemetrySync {
					t.Error("Wrong call")
				}
			case 5:
				if resource != TokenSync {
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

	telemetryService.RecordSuccessfulSync(SplitSync)
	telemetryService.RecordSuccessfulSync(SegmentSync)
	telemetryService.RecordSuccessfulSync(ImpressionSync)
	telemetryService.RecordSuccessfulSync(EventSync)
	telemetryService.RecordSuccessfulSync(TelemetrySync)
	telemetryService.RecordSuccessfulSync(TokenSync)
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
				if resource != SplitSync || status != 500 {
					t.Error("Wrong call")
				}
			case 1:
				if resource != SegmentSync || status != 401 {
					t.Error("Wrong call")
				}
			case 2:
				if resource != ImpressionSync || status != 402 {
					t.Error("Wrong call")
				}
			case 3:
				if resource != EventSync || status != 400 {
					t.Error("Wrong call")
				}
			case 4:
				if resource != TelemetrySync || status != 404 {
					t.Error("Wrong call")
				}
			case 5:
				if resource != TokenSync || status != 403 {
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
				if resource != SplitSync || bucket != 0 {
					t.Error("Unexpected method call")
				}
			case 1:
				if resource != SegmentSync || bucket != 1 {
					t.Error("Unexpected method call")
				}
			case 2:
				if resource != ImpressionSync || bucket != 4 {
					t.Error("Unexpected method call")
				}
			case 3:
				if resource != EventSync || bucket != 5 {
					t.Error("Unexpected method call")
				}
			case 4:
				if resource != TelemetrySync || bucket != 6 {
					t.Error("Unexpected method call")
				}
			case 5:
				if resource != TokenSync || bucket != 8 {
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

	telemetryService.RecordSyncError(SplitSync, 500)
	telemetryService.RecordSyncError(SegmentSync, 401)
	telemetryService.RecordSyncError(ImpressionSync, 402)
	telemetryService.RecordSyncError(EventSync, 400)
	telemetryService.RecordSyncError(TelemetrySync, 404)
	telemetryService.RecordSyncError(TokenSync, 403)
	if called != 6 {
		t.Error("It should be called six times")
	}

	telemetryService.PopHTTPErrors()
	if getCalled != 1 {
		t.Error("It should be called once")
	}

	telemetryService.RecordSyncLatency(SplitSync, (500 * time.Nanosecond).Nanoseconds())
	telemetryService.RecordSyncLatency(SegmentSync, (1500 * time.Nanosecond).Nanoseconds())
	telemetryService.RecordSyncLatency(ImpressionSync, (3500 * time.Nanosecond).Nanoseconds())
	telemetryService.RecordSyncLatency(EventSync, (5500 * time.Nanosecond).Nanoseconds())
	telemetryService.RecordSyncLatency(TelemetrySync, (8000 * time.Nanosecond).Nanoseconds())
	telemetryService.RecordSyncLatency(TokenSync, (20000 * time.Nanosecond).Nanoseconds())
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
				if streamingEvent.Type != EventTypeSSEConnectionEstablished || streamingEvent.Data != 0 {
					t.Error("Unexpected streamingEvent")
				}
			case 1:
				if streamingEvent.Type != EventTypeOccupancyPri || streamingEvent.Data != 10 {
					t.Error("Unexpected streamingEvent")
				}
			case 2:
				if streamingEvent.Type != EventTypeOccupancySec || streamingEvent.Data != 1 {
					t.Error("Unexpected streamingEvent")
				}
			case 3:
				if streamingEvent.Type != EventTypeStreamingStatus || streamingEvent.Data != StreamingDisabled {
					t.Error("Unexpected streamingEvent")
				}
			case 4:
				if streamingEvent.Type != EventTypeConnectionError || streamingEvent.Data != NonRequested {
					t.Error("Unexpected streamingEvent")
				}
			case 5:
				if streamingEvent.Type != EventTypeTokenRefresh || streamingEvent.Data != 123456789 {
					t.Error("Unexpected streamingEvent")
				}
			case 6:
				if streamingEvent.Type != EventTypeAblyError || streamingEvent.Data != 40010 {
					t.Error("Unexpected streamingEvent")
				}
			case 7:
				if streamingEvent.Type != EventTypeSyncMode || streamingEvent.Data != Polling {
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

	telemetryService.RecordStreamingEvent(EventTypeSSEConnectionEstablished, 0)
	telemetryService.RecordStreamingEvent(EventTypeOccupancyPri, 10)
	telemetryService.RecordStreamingEvent(EventTypeOccupancySec, 1)
	telemetryService.RecordStreamingEvent(EventTypeStreamingStatus, StreamingDisabled)
	telemetryService.RecordStreamingEvent(EventTypeConnectionError, NonRequested)
	telemetryService.RecordStreamingEvent(EventTypeTokenRefresh, 123456789)
	telemetryService.RecordStreamingEvent(EventTypeAblyError, 40010)
	telemetryService.RecordStreamingEvent(EventTypeSyncMode, Polling)
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
