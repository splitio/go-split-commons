package synchronizer

import (
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v6/conf"
	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/flagsets"
	hcMock "github.com/splitio/go-split-commons/v6/healthcheck/mocks"
	"github.com/splitio/go-split-commons/v6/push"
	"github.com/splitio/go-split-commons/v6/service"
	"github.com/splitio/go-split-commons/v6/service/api"
	httpMocks "github.com/splitio/go-split-commons/v6/service/mocks"
	"github.com/splitio/go-split-commons/v6/storage/inmemory"
	storageMock "github.com/splitio/go-split-commons/v6/storage/mocks"
	syncMocks "github.com/splitio/go-split-commons/v6/synchronizer/mocks"
	"github.com/splitio/go-split-commons/v6/synchronizer/worker/event"
	"github.com/splitio/go-split-commons/v6/synchronizer/worker/impression"
	"github.com/splitio/go-split-commons/v6/synchronizer/worker/segment"
	"github.com/splitio/go-split-commons/v6/synchronizer/worker/split"
	"github.com/splitio/go-split-commons/v6/tasks"
	"github.com/splitio/go-split-commons/v6/telemetry"
	"github.com/splitio/go-toolkit/v5/common"
	"github.com/splitio/go-toolkit/v5/datastructures/set"
	"github.com/splitio/go-toolkit/v5/logging"
)

func validReqParams(t *testing.T, fetchOptions service.RequestParams) {
	req, _ := http.NewRequest("GET", "test", nil)
	fetchOptions.Apply(req)
	if req.Header.Get("Cache-Control") != "no-cache" {
		t.Error("Wrong header")
	}
	if req.URL.Query().Get("since") != "-1" {
		t.Error("Wrong since")
	}
}

func TestSyncAllErrorSplits(t *testing.T) {
	var splitFetchCalled int64
	var notifyEventCalled int64
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitAPI := api.SplitAPI{
		SplitFetcher: httpMocks.MockSplitFetcher{
			FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
				atomic.AddInt64(&splitFetchCalled, 1)
				validReqParams(t, fetchOptions)
				return nil, errors.New("Some")
			},
		},
	}
	splitMockStorage := storageMock.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) { return -1, nil },
	}
	telemetryMockStorage := storageMock.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, time time.Time) {},
	}
	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&notifyEventCalled, 1)
		},
	}
	advanced := conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100, SegmentQueueSize: 50, SegmentWorkers: 5}
	workers := Workers{
		SplitUpdater:       split.NewSplitUpdater(splitMockStorage, splitAPI.SplitFetcher, logger, telemetryMockStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil)),
		SegmentUpdater:     segment.NewSegmentUpdater(splitMockStorage, storageMock.MockSegmentStorage{}, splitAPI.SegmentFetcher, logger, telemetryMockStorage, appMonitorMock),
		EventRecorder:      event.NewEventRecorderSingle(storageMock.MockEventStorage{}, splitAPI.EventRecorder, logger, dtos.Metadata{}, telemetryMockStorage),
		ImpressionRecorder: impression.NewRecorderSingle(storageMock.MockImpressionStorage{}, splitAPI.ImpressionRecorder, logger, dtos.Metadata{}, conf.ImpressionsModeDebug, telemetryMockStorage),
		TelemetryRecorder:  telemetry.NewTelemetrySynchronizer(telemetryMockStorage, nil, nil, nil, nil, dtos.Metadata{}, telemetryMockStorage),
	}
	splitTasks := SplitTasks{
		EventSyncTask:      tasks.NewRecordEventsTask(workers.EventRecorder, advanced.EventsBulkSize, 10, logger),
		ImpressionSyncTask: tasks.NewRecordImpressionsTask(workers.ImpressionRecorder, 10, logger, advanced.ImpressionsBulkSize),
		SegmentSyncTask:    tasks.NewFetchSegmentsTask(workers.SegmentUpdater, 10, advanced.SegmentWorkers, advanced.SegmentQueueSize, logger, appMonitorMock),
		SplitSyncTask:      tasks.NewFetchSplitsTask(workers.SplitUpdater, 10, logger),
		TelemetrySyncTask:  tasks.NewRecordTelemetryTask(workers.TelemetryRecorder, 10, logger),
	}
	syncForTest := NewSynchronizer(advanced, splitTasks, workers, logger, nil)
	err := syncForTest.SyncAll()
	if err == nil {
		t.Error("It should return error")
	}
	if atomic.LoadInt64(&splitFetchCalled) != 1 {
		t.Error("It should be called once")
	}
	if atomic.LoadInt64(&notifyEventCalled) != 1 {
		t.Error("It should be called once")
	}
}

func TestSyncAllErrorInSegments(t *testing.T) {
	var splitFetchCalled int64
	var segmentFetchCalled int64
	var notifyEventCalled int64
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}
	mockedSplit2 := dtos.SplitDTO{Name: "split2", Killed: true, Status: "ACTIVE", TrafficTypeName: "two"}
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitAPI := api.SplitAPI{
		SplitFetcher: httpMocks.MockSplitFetcher{
			FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
				atomic.AddInt64(&splitFetchCalled, 1)
				validReqParams(t, fetchOptions)
				return &dtos.SplitChangesDTO{
					Splits: []dtos.SplitDTO{mockedSplit1, mockedSplit2},
					Since:  3,
					Till:   3,
				}, nil
			},
		},
		SegmentFetcher: httpMocks.MockSegmentFetcher{
			FetchCall: func(name string, fetchOptions *service.SegmentRequestParams) (*dtos.SegmentChangesDTO, error) {
				atomic.AddInt64(&segmentFetchCalled, 1)
				validReqParams(t, fetchOptions)
				if name != "segment1" && name != "segment2" {
					t.Error("Wrong name")
				}
				return nil, errors.New("some")
			},
		},
	}
	splitMockStorage := storageMock.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) { return -1, nil },
		UpdateCall: func(toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO, changeNumber int64) {
			if changeNumber != 3 {
				t.Error("Wrong changenumber")
			}
			if len(toAdd) != 2 {
				t.Error("Wrong length of passed splits")
			}
		},
		SegmentNamesCall: func() *set.ThreadUnsafeSet { return set.NewSet("segment1", "segment2") },
	}
	segmentMockStorage := storageMock.MockSegmentStorage{
		ChangeNumberCall: func(segmentName string) (int64, error) { return -1, nil },
	}
	telemetryMockStorage := storageMock.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, time time.Time) {},
		RecordSyncLatencyCall:    func(resource int, latency time.Duration) {},
	}
	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&notifyEventCalled, 1)
		},
	}
	advanced := conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100, SegmentQueueSize: 50, SegmentWorkers: 5}
	workers := Workers{
		SplitUpdater:       split.NewSplitUpdater(splitMockStorage, splitAPI.SplitFetcher, logger, telemetryMockStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil)),
		SegmentUpdater:     segment.NewSegmentUpdater(splitMockStorage, segmentMockStorage, splitAPI.SegmentFetcher, logger, telemetryMockStorage, appMonitorMock),
		EventRecorder:      event.NewEventRecorderSingle(storageMock.MockEventStorage{}, splitAPI.EventRecorder, logger, dtos.Metadata{}, telemetryMockStorage),
		ImpressionRecorder: impression.NewRecorderSingle(storageMock.MockImpressionStorage{}, splitAPI.ImpressionRecorder, logger, dtos.Metadata{}, conf.ImpressionsModeDebug, telemetryMockStorage),
		TelemetryRecorder:  telemetry.NewTelemetrySynchronizer(telemetryMockStorage, nil, nil, nil, nil, dtos.Metadata{}, telemetryMockStorage),
	}
	splitTasks := SplitTasks{
		EventSyncTask:      tasks.NewRecordEventsTask(workers.EventRecorder, advanced.EventsBulkSize, 10, logger),
		ImpressionSyncTask: tasks.NewRecordImpressionsTask(workers.ImpressionRecorder, 10, logger, advanced.ImpressionsBulkSize),
		SegmentSyncTask:    tasks.NewFetchSegmentsTask(workers.SegmentUpdater, 10, advanced.SegmentWorkers, advanced.SegmentQueueSize, logger, appMonitorMock),
		SplitSyncTask:      tasks.NewFetchSplitsTask(workers.SplitUpdater, 10, logger),
		TelemetrySyncTask:  tasks.NewRecordTelemetryTask(workers.TelemetryRecorder, 10, logger),
	}
	syncForTest := NewSynchronizer(advanced, splitTasks, workers, logger, nil)
	err := syncForTest.SyncAll()
	if err == nil {
		t.Error("It should return error")
	}
	if atomic.LoadInt64(&splitFetchCalled) != 1 {
		t.Error("It should be called once")
	}
	if atomic.LoadInt64(&segmentFetchCalled) != 2 {
		t.Error("It should be called twice")
	}
	if atomic.LoadInt64(&notifyEventCalled) < 1 {
		t.Error("It should be called at least once")
	}
}

func TestSyncAllOk(t *testing.T) {
	var splitFetchCalled int64
	var segmentFetchCalled int64
	var notifyEventCalled int64
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}
	mockedSplit2 := dtos.SplitDTO{Name: "split2", Killed: true, Status: "ACTIVE", TrafficTypeName: "two"}
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitAPI := api.SplitAPI{
		SplitFetcher: httpMocks.MockSplitFetcher{
			FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
				atomic.AddInt64(&splitFetchCalled, 1)
				validReqParams(t, fetchOptions)
				return &dtos.SplitChangesDTO{
					Splits: []dtos.SplitDTO{mockedSplit1, mockedSplit2},
					Since:  3,
					Till:   3,
				}, nil
			},
		},
		SegmentFetcher: httpMocks.MockSegmentFetcher{
			FetchCall: func(name string, fetchOptions *service.SegmentRequestParams) (*dtos.SegmentChangesDTO, error) {
				atomic.AddInt64(&segmentFetchCalled, 1)
				if name != "segment1" && name != "segment2" {
					t.Error("Wrong name")
				}
				validReqParams(t, fetchOptions)
				return &dtos.SegmentChangesDTO{
					Name:    name,
					Added:   []string{"some"},
					Removed: []string{},
					Since:   123,
					Till:    123,
				}, nil
			},
		},
	}
	splitMockStorage := storageMock.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) { return -1, nil },
		UpdateCall: func(toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO, changeNumber int64) {
			if changeNumber != 3 {
				t.Error("Wrong changenumber")
			}
			if len(toAdd) != 2 {
				t.Error("Wrong length of passed splits")
			}
		},
		SegmentNamesCall: func() *set.ThreadUnsafeSet { return set.NewSet("segment1", "segment2") },
	}
	segmentMockStorage := storageMock.MockSegmentStorage{
		ChangeNumberCall: func(segmentName string) (int64, error) { return -1, nil },
		KeysCall: func(segmentName string) *set.ThreadUnsafeSet {
			if segmentName != "segment1" && segmentName != "segment2" {
				t.Error("Wrong name")
			}
			return nil
		},
		UpdateCall: func(name string, toAdd *set.ThreadUnsafeSet, toRemove *set.ThreadUnsafeSet, changeNumber int64) error {
			if name != "segment1" && name != "segment2" {
				t.Error("Wrong name")
			}
			return nil
		},
	}
	telemetryMockStorage := storageMock.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, time time.Time) {},
		RecordSyncLatencyCall:    func(resource int, latency time.Duration) {},
	}
	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&notifyEventCalled, 1)
		},
	}
	advanced := conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100, SegmentQueueSize: 50, SegmentWorkers: 5}
	workers := Workers{
		SplitUpdater:        split.NewSplitUpdater(splitMockStorage, splitAPI.SplitFetcher, logger, telemetryMockStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil)),
		SegmentUpdater:      segment.NewSegmentUpdater(splitMockStorage, segmentMockStorage, splitAPI.SegmentFetcher, logger, telemetryMockStorage, appMonitorMock),
		EventRecorder:       event.NewEventRecorderSingle(storageMock.MockEventStorage{}, splitAPI.EventRecorder, logger, dtos.Metadata{}, telemetryMockStorage),
		ImpressionRecorder:  impression.NewRecorderSingle(storageMock.MockImpressionStorage{}, splitAPI.ImpressionRecorder, logger, dtos.Metadata{}, conf.ImpressionsModeDebug, telemetryMockStorage),
		TelemetryRecorder:   telemetry.NewTelemetrySynchronizer(telemetryMockStorage, nil, nil, nil, nil, dtos.Metadata{}, telemetryMockStorage),
		LargeSegmentUpdater: syncMocks.MockLargeSegmentUpdater{SynchronizeLargeSegmentsCall: func() error { return nil }},
	}
	splitTasks := SplitTasks{
		EventSyncTask:      tasks.NewRecordEventsTask(workers.EventRecorder, advanced.EventsBulkSize, 10, logger),
		ImpressionSyncTask: tasks.NewRecordImpressionsTask(workers.ImpressionRecorder, 10, logger, advanced.ImpressionsBulkSize),
		SegmentSyncTask:    tasks.NewFetchSegmentsTask(workers.SegmentUpdater, 10, advanced.SegmentWorkers, advanced.SegmentQueueSize, logger, appMonitorMock),
		SplitSyncTask:      tasks.NewFetchSplitsTask(workers.SplitUpdater, 10, logger),
		TelemetrySyncTask:  tasks.NewRecordTelemetryTask(workers.TelemetryRecorder, 10, logger),
	}
	syncForTest := NewSynchronizer(advanced, splitTasks, workers, logger, nil)
	err := syncForTest.SyncAll()
	if err != nil {
		t.Error("It should not return error")
	}
	if splitFetchCalled != 1 {
		t.Error("It should be called once")
	}
	if segmentFetchCalled != 2 {
		t.Error("It should be called twice")
	}
	if atomic.LoadInt64(&notifyEventCalled) < 1 {
		t.Error("It should be called at least once")
	}
}

func TestPeriodicFetching(t *testing.T) {
	var splitFetchCalled int64
	var segmentFetchCalled int64
	var notifyEventCalled int64
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}
	mockedSplit2 := dtos.SplitDTO{Name: "split2", Killed: true, Status: "ACTIVE", TrafficTypeName: "two"}
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitAPI := api.SplitAPI{
		SplitFetcher: httpMocks.MockSplitFetcher{
			FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
				atomic.AddInt64(&splitFetchCalled, 1)
				validReqParams(t, fetchOptions)
				return &dtos.SplitChangesDTO{
					Splits: []dtos.SplitDTO{mockedSplit1, mockedSplit2},
					Since:  3,
					Till:   3,
				}, nil
			},
		},
		SegmentFetcher: httpMocks.MockSegmentFetcher{
			FetchCall: func(name string, fetchOptions *service.SegmentRequestParams) (*dtos.SegmentChangesDTO, error) {
				atomic.AddInt64(&segmentFetchCalled, 1)
				if name != "segment1" && name != "segment2" {
					t.Error("Wrong name")
				}
				validReqParams(t, fetchOptions)
				return &dtos.SegmentChangesDTO{
					Name:    name,
					Added:   []string{"some"},
					Removed: []string{},
					Since:   123,
					Till:    123,
				}, nil
			},
		},
	}
	splitMockStorage := storageMock.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) { return -1, nil },
		UpdateCall: func(toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO, changeNumber int64) {
			if changeNumber != 3 {
				t.Error("Wrong changenumber")
			}
			if len(toAdd) != 2 {
				t.Error("Wrong length of passed splits")
			}
		},
		SegmentNamesCall: func() *set.ThreadUnsafeSet { return set.NewSet("segment1", "segment2") },
	}
	segmentMockStorage := storageMock.MockSegmentStorage{
		ChangeNumberCall: func(segmentName string) (int64, error) { return -1, nil },
		KeysCall: func(segmentName string) *set.ThreadUnsafeSet {
			if segmentName != "segment1" && segmentName != "segment2" {
				t.Error("Wrong name")
			}
			return nil
		},
		UpdateCall: func(name string, toAdd *set.ThreadUnsafeSet, toRemove *set.ThreadUnsafeSet, changeNumber int64) error {
			if name != "segment1" && name != "segment2" {
				t.Error("Wrong name")
			}
			return nil
		},
	}
	telemetryMockStorage := storageMock.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, time time.Time) {},
		RecordSyncLatencyCall:    func(resource int, latency time.Duration) {},
	}
	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&notifyEventCalled, 1)
		},
		ResetCall: func(counterType, value int) {},
	}
	advanced := conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100, SegmentQueueSize: 50, SegmentWorkers: 5}
	workers := Workers{
		SplitUpdater:       split.NewSplitUpdater(splitMockStorage, splitAPI.SplitFetcher, logger, telemetryMockStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil)),
		SegmentUpdater:     segment.NewSegmentUpdater(splitMockStorage, segmentMockStorage, splitAPI.SegmentFetcher, logger, telemetryMockStorage, appMonitorMock),
		EventRecorder:      event.NewEventRecorderSingle(storageMock.MockEventStorage{}, splitAPI.EventRecorder, logger, dtos.Metadata{}, telemetryMockStorage),
		ImpressionRecorder: impression.NewRecorderSingle(storageMock.MockImpressionStorage{}, splitAPI.ImpressionRecorder, logger, dtos.Metadata{}, conf.ImpressionsModeDebug, telemetryMockStorage),
		TelemetryRecorder:  telemetry.NewTelemetrySynchronizer(telemetryMockStorage, nil, nil, nil, nil, dtos.Metadata{}, telemetryMockStorage),
	}
	splitTasks := SplitTasks{
		EventSyncTask:      tasks.NewRecordEventsTask(workers.EventRecorder, advanced.EventsBulkSize, 1, logger),
		ImpressionSyncTask: tasks.NewRecordImpressionsTask(workers.ImpressionRecorder, 1, logger, advanced.ImpressionsBulkSize),
		SegmentSyncTask:    tasks.NewFetchSegmentsTask(workers.SegmentUpdater, 1, advanced.SegmentWorkers, advanced.SegmentQueueSize, logger, appMonitorMock),
		SplitSyncTask:      tasks.NewFetchSplitsTask(workers.SplitUpdater, 1, logger),
		TelemetrySyncTask:  tasks.NewRecordTelemetryTask(workers.TelemetryRecorder, 10, logger),
	}
	syncForTest := NewSynchronizer(advanced, splitTasks, workers, logger, nil)
	syncForTest.StartPeriodicFetching()
	time.Sleep(time.Millisecond * 2200)
	if atomic.LoadInt64(&splitFetchCalled) < 2 {
		t.Error("It should be called twice")
	}
	if atomic.LoadInt64(&segmentFetchCalled) < 2 {
		t.Error("It should be called twice")
	}
	if atomic.LoadInt64(&notifyEventCalled) < 1 {
		t.Error("It should be called at least once")
	}
	syncForTest.StopPeriodicFetching()
	t.Error("hola")
}

func TestPeriodicRecording(t *testing.T) {
	var impressionsCalled int64
	var eventsCalled int64
	var statsCalled int64
	var notifyEventCalled int64
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitAPI := api.SplitAPI{
		EventRecorder: httpMocks.MockEventRecorder{
			RecordCall: func(events []dtos.EventDTO, metadata dtos.Metadata) error {
				atomic.AddInt64(&eventsCalled, 1)
				if len(events) != 1 {
					t.Error("Wrong length")
				}
				return nil
			},
		},
		ImpressionRecorder: httpMocks.MockImpressionRecorder{
			RecordCall: func(impressions []dtos.ImpressionsDTO, metadata dtos.Metadata, extraHeaders map[string]string) error {
				atomic.AddInt64(&impressionsCalled, 1)
				if len(impressions) != 1 {
					t.Error("Wrong length")
				}
				return nil
			},
		},
		TelemetryRecorder: httpMocks.MockTelemetryRecorder{
			RecordStatsCall: func(stats dtos.Stats, metadata dtos.Metadata) error {
				atomic.AddInt64(&statsCalled, 1)
				return nil
			},
		},
	}
	impressionMockStorage := storageMock.MockImpressionStorage{
		PopNCall: func(n int64) ([]dtos.Impression, error) {
			if n != 100 {
				t.Error("It should be 100")
			}
			return []dtos.Impression{{
				BucketingKey: "someBucketingKey",
				ChangeNumber: 123456789,
				FeatureName:  "someFeature",
				KeyName:      "someKey",
				Label:        "someLabel",
				Time:         123456789,
				Treatment:    "someTreatment",
			}}, nil
		},
		EmptyCall: func() bool { return impressionsCalled >= 3 },
	}
	eventMockStorage := storageMock.MockEventStorage{
		PopNCall: func(n int64) ([]dtos.EventDTO, error) {
			if n != 100 {
				t.Error("It should be 100")
			}
			return []dtos.EventDTO{{
				EventTypeID:     "someEvent",
				Key:             "someKey",
				Properties:      nil,
				Timestamp:       123456789,
				TrafficTypeName: "someTrafficType",
				Value:           nil,
			}}, nil
		},
		EmptyCall: func() bool { return eventsCalled >= 4 },
	}
	telemetryMockStorage := storageMock.MockTelemetryStorage{
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
		RecordSuccessfulSyncCall:   func(resource int, time time.Time) {},
		RecordSyncLatencyCall:      func(resource int, latency time.Duration) {},
		PopUpdatesFromSSECall:      func() dtos.UpdatesFromSSE { return dtos.UpdatesFromSSE{} },
	}
	splitMockStorage := storageMock.MockSplitStorage{
		SplitNamesCall:   func() []string { return []string{} },
		SegmentNamesCall: func() *set.ThreadUnsafeSet { return set.NewSet() },
	}
	segmentMockStorage := storageMock.MockSegmentStorage{
		SegmentKeysCountCall: func() int64 { return 30 },
	}
	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&notifyEventCalled, 1)
		},
	}
	advanced := conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100, SegmentQueueSize: 50, SegmentWorkers: 5}
	workers := Workers{
		SplitUpdater:       split.NewSplitUpdater(splitMockStorage, splitAPI.SplitFetcher, logger, telemetryMockStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil)),
		SegmentUpdater:     segment.NewSegmentUpdater(splitMockStorage, segmentMockStorage, splitAPI.SegmentFetcher, logger, telemetryMockStorage, appMonitorMock),
		EventRecorder:      event.NewEventRecorderSingle(eventMockStorage, splitAPI.EventRecorder, logger, dtos.Metadata{}, telemetryMockStorage),
		ImpressionRecorder: impression.NewRecorderSingle(impressionMockStorage, splitAPI.ImpressionRecorder, logger, dtos.Metadata{}, conf.ImpressionsModeDebug, telemetryMockStorage),
		TelemetryRecorder:  telemetry.NewTelemetrySynchronizer(telemetryMockStorage, splitAPI.TelemetryRecorder, splitMockStorage, segmentMockStorage, logger, dtos.Metadata{}, telemetryMockStorage),
	}
	splitTasks := SplitTasks{
		EventSyncTask:      tasks.NewRecordEventsTask(workers.EventRecorder, advanced.EventsBulkSize, 1, logger),
		ImpressionSyncTask: tasks.NewRecordImpressionsTask(workers.ImpressionRecorder, 1, logger, advanced.ImpressionsBulkSize),
		SegmentSyncTask:    tasks.NewFetchSegmentsTask(workers.SegmentUpdater, 1, advanced.SegmentWorkers, advanced.SegmentQueueSize, logger, appMonitorMock),
		SplitSyncTask:      tasks.NewFetchSplitsTask(workers.SplitUpdater, 1, logger),
		TelemetrySyncTask:  tasks.NewRecordTelemetryTask(workers.TelemetryRecorder, 1, logger),
	}
	workers.TelemetryRecorder.SynchronizeStats()
	syncForTest := NewSynchronizer(advanced, splitTasks, workers, logger, nil)
	syncForTest.StartPeriodicDataRecording()
	time.Sleep(time.Second * 2)
	if atomic.LoadInt64(&impressionsCalled) < 1 {
		t.Error("It should be called once")
	}
	if atomic.LoadInt64(&eventsCalled) < 1 {
		t.Error("It should be called once")
	}
	if atomic.LoadInt64(&statsCalled) < 1 {
		t.Error("It should be called once")
	}
	syncForTest.StopPeriodicDataRecording()
	time.Sleep(time.Second * 1)
	if atomic.LoadInt64(&impressionsCalled) < 2 {
		t.Error("It should be called two times")
	}
	if atomic.LoadInt64(&eventsCalled) < 2 {
		t.Error("It should be called two times")
	}
	if atomic.LoadInt64(&statsCalled) < 2 {
		t.Error("It should be called two times")
	}
	if atomic.LoadInt64(&notifyEventCalled) != 0 {
		t.Error("It should not be called")
	}
}

func TestSplitUpdateWorkerCNGreaterThanFFChange(t *testing.T) {
	var splitFetchCalled int64
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitAPI := api.SplitAPI{
		SplitFetcher: httpMocks.MockSplitFetcher{
			FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
				atomic.AddInt64(&splitFetchCalled, 1)
				return nil, nil
			},
		},
	}
	splitMockStorage := storageMock.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return 2, nil
		},
	}
	segmentMockStorage := storageMock.MockSegmentStorage{}
	telemetryMockStorage := storageMock.MockTelemetryStorage{}
	appMonitorMock := hcMock.MockApplicationMonitor{}

	workers := Workers{
		SplitUpdater:   split.NewSplitUpdater(splitMockStorage, splitAPI.SplitFetcher, logger, telemetryMockStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil)),
		SegmentUpdater: segment.NewSegmentUpdater(splitMockStorage, segmentMockStorage, splitAPI.SegmentFetcher, logger, telemetryMockStorage, appMonitorMock),
	}
	splitTasks := SplitTasks{
		SegmentSyncTask: tasks.NewFetchSegmentsTask(workers.SegmentUpdater, 1, 1, 500, logger, appMonitorMock),
		SplitSyncTask:   tasks.NewFetchSplitsTask(workers.SplitUpdater, 1, logger),
	}
	syncForTest := NewSynchronizer(conf.AdvancedConfig{}, splitTasks, workers, logger, nil)

	splitQueue := make(chan dtos.SplitChangeUpdate, 5000)
	splitWorker, _ := push.NewSplitUpdateWorker(splitQueue, syncForTest, logger)
	splitWorker.Start()

	// Testing Storage With Changenumber Greater Than FF
	splitQueue <- *dtos.NewSplitChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "some"), 1), nil, nil,
	)

	time.Sleep(300 * time.Millisecond)
	if !splitWorker.IsRunning() {
		t.Error("It should be running")
	}

	if c := atomic.LoadInt64(&splitFetchCalled); c != 0 {
		t.Error("should haven't been called. got: ", c)
	}
}

func TestSplitUpdateWorkerStorageCNEqualsFFCN(t *testing.T) {
	var splitFetchCalled int64
	var updateCalled int64
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitAPI := api.SplitAPI{
		SplitFetcher: httpMocks.MockSplitFetcher{
			FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
				atomic.AddInt64(&splitFetchCalled, 1)
				return nil, nil
			},
		},
	}
	splitMockStorage := storageMock.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return 2, nil
		},
		UpdateCall: func(toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO, changeNumber int64) {
			atomic.AddInt64(&updateCalled, 1)
		},
	}
	segmentMockStorage := storageMock.MockSegmentStorage{}
	telemetryMockStorage := storageMock.MockTelemetryStorage{}
	appMonitorMock := hcMock.MockApplicationMonitor{}

	workers := Workers{
		SplitUpdater:   split.NewSplitUpdater(splitMockStorage, splitAPI.SplitFetcher, logger, telemetryMockStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil)),
		SegmentUpdater: segment.NewSegmentUpdater(splitMockStorage, segmentMockStorage, splitAPI.SegmentFetcher, logger, telemetryMockStorage, appMonitorMock),
	}
	splitTasks := SplitTasks{
		SegmentSyncTask: tasks.NewFetchSegmentsTask(workers.SegmentUpdater, 1, 1, 500, logger, appMonitorMock),
		SplitSyncTask:   tasks.NewFetchSplitsTask(workers.SplitUpdater, 1, logger),
	}
	syncForTest := NewSynchronizer(conf.AdvancedConfig{}, splitTasks, workers, logger, nil)

	splitQueue := make(chan dtos.SplitChangeUpdate, 5000)
	splitWorker, _ := push.NewSplitUpdateWorker(splitQueue, syncForTest, logger)
	splitWorker.Start()

	featureFlag := dtos.SplitDTO{ChangeNumber: 2, Status: split.Active}
	splitQueue <- *dtos.NewSplitChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "some"), 2),
		common.Int64Ref(2), &featureFlag,
	)

	time.Sleep(300 * time.Millisecond)
	if !splitWorker.IsRunning() {
		t.Error("It should be running")
	}

	if c := atomic.LoadInt64(&splitFetchCalled); c != 0 {
		t.Error("should haven't been called. got: ", c)
	}
	if u := atomic.LoadInt64(&updateCalled); u != 0 {
		t.Error("should haven't been called. got: ", u)
	}
}

func TestSplitUpdateWorkerFFPcnEqualsFFNotNil(t *testing.T) {
	var splitFetchCalled int64
	var updateCalled int64
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitAPI := api.SplitAPI{
		SplitFetcher: httpMocks.MockSplitFetcher{
			FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
				atomic.AddInt64(&splitFetchCalled, 1)
				return nil, nil
			},
		},
	}
	splitMockStorage := storageMock.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return 2, nil
		},
		UpdateCall: func(toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO, changeNumber int64) {
			if changeNumber != 4 {
				t.Error("Wrong changeNumber")
			}
			atomic.AddInt64(&updateCalled, 1)
		},
	}
	segmentMockStorage := storageMock.MockSegmentStorage{}
	telemetryStorage, _ := inmemory.NewTelemetryStorage()
	appMonitorMock := hcMock.MockApplicationMonitor{}

	workers := Workers{
		SplitUpdater:   split.NewSplitUpdater(splitMockStorage, splitAPI.SplitFetcher, logger, telemetryStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil)),
		SegmentUpdater: segment.NewSegmentUpdater(splitMockStorage, segmentMockStorage, splitAPI.SegmentFetcher, logger, telemetryStorage, appMonitorMock),
	}
	splitTasks := SplitTasks{
		SegmentSyncTask: tasks.NewFetchSegmentsTask(workers.SegmentUpdater, 1, 1, 500, logger, appMonitorMock),
		SplitSyncTask:   tasks.NewFetchSplitsTask(workers.SplitUpdater, 1, logger),
	}
	syncForTest := NewSynchronizer(conf.AdvancedConfig{}, splitTasks, workers, logger, nil)

	splitQueue := make(chan dtos.SplitChangeUpdate, 5000)
	splitWorker, _ := push.NewSplitUpdateWorker(splitQueue, syncForTest, logger)
	splitWorker.Start()

	featureFlag := dtos.SplitDTO{ChangeNumber: 4, Status: split.Active}
	splitQueue <- *dtos.NewSplitChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "some"), 4),
		common.Int64Ref(2), &featureFlag,
	)

	time.Sleep(300 * time.Millisecond)
	if !splitWorker.IsRunning() {
		t.Error("It should be running")
	}

	if c := atomic.LoadInt64(&splitFetchCalled); c != 0 {
		t.Error("should haven't been called. got: ", c)
	}
	if u := atomic.LoadInt64(&updateCalled); u != 1 {
		t.Error("should have been called once. got: ", u)
	}
	updatesFromSSE := telemetryStorage.PopUpdatesFromSSE()
	if updatesFromSSE.Splits != 1 {
		t.Error("It should track 1 splitUpdate")
	}
}

func TestSplitUpdateWorkerGetCNFromStorageError(t *testing.T) {
	var splitFetchCalled int64
	var updateCalled int64
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitAPI := api.SplitAPI{
		SplitFetcher: httpMocks.MockSplitFetcher{
			FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
				atomic.AddInt64(&splitFetchCalled, 1)
				if fetchOptions.ChangeNumber() != 0 {
					t.Error("Wrong changenumber passed")
				}
				return &dtos.SplitChangesDTO{
					Till:   2,
					Since:  2,
					Splits: []dtos.SplitDTO{},
				}, nil
			},
		},
	}
	splitMockStorage := storageMock.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return 0, errors.New("error getting change number")
		},
		UpdateCall: func(toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO, changeNumber int64) {
			atomic.AddInt64(&updateCalled, 1)
			if changeNumber != 2 {
				t.Error("changenumber should be 2")
			}
			if len(toAdd) != 0 {
				t.Error("toAdd should have one feature flag")
			}
			if len(toRemove) != 0 {
				t.Error("toRemove should be empty")
			}
		},
	}
	segmentMockStorage := storageMock.MockSegmentStorage{}
	telemetryMockStorage := storageMock.MockTelemetryStorage{
		RecordSyncLatencyCall:    func(resource int, latency time.Duration) {},
		RecordSuccessfulSyncCall: func(resource int, when time.Time) {},
	}
	hcMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {},
	}

	workers := Workers{
		SplitUpdater:   split.NewSplitUpdater(splitMockStorage, splitAPI.SplitFetcher, logger, telemetryMockStorage, hcMonitorMock, flagsets.NewFlagSetFilter(nil)),
		SegmentUpdater: segment.NewSegmentUpdater(splitMockStorage, segmentMockStorage, splitAPI.SegmentFetcher, logger, telemetryMockStorage, hcMonitorMock),
	}
	splitTasks := SplitTasks{
		SegmentSyncTask: tasks.NewFetchSegmentsTask(workers.SegmentUpdater, 1, 1, 500, logger, hcMonitorMock),
		SplitSyncTask:   tasks.NewFetchSplitsTask(workers.SplitUpdater, 1, logger),
	}
	syncForTest := NewSynchronizer(conf.AdvancedConfig{}, splitTasks, workers, logger, nil)

	splitQueue := make(chan dtos.SplitChangeUpdate, 5000)
	splitWorker, _ := push.NewSplitUpdateWorker(splitQueue, syncForTest, logger)
	splitWorker.Start()

	featureFlag := dtos.SplitDTO{ChangeNumber: 4, Status: split.Active}
	splitQueue <- *dtos.NewSplitChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "some"), 4),
		common.Int64Ref(2), &featureFlag,
	)

	time.Sleep(300 * time.Millisecond)
	if !splitWorker.IsRunning() {
		t.Error("It should be running")
	}

	if c := atomic.LoadInt64(&splitFetchCalled); c != 1 {
		t.Error("should have been called once. got: ", c)
	}
	if u := atomic.LoadInt64(&updateCalled); u != 1 {
		t.Error("should have been called once. got: ", u)
	}
}

func TestSplitUpdateWorkerFFIsNil(t *testing.T) {
	var splitFetchCalled int64
	var updateCalled int64
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitAPI := api.SplitAPI{
		SplitFetcher: httpMocks.MockSplitFetcher{
			FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
				atomic.AddInt64(&splitFetchCalled, 1)
				return &dtos.SplitChangesDTO{
					Till:   4,
					Since:  4,
					Splits: []dtos.SplitDTO{},
				}, nil
			},
		},
	}
	splitMockStorage := storageMock.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return 2, nil
		},
		UpdateCall: func(toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO, changeNumber int64) {
			if changeNumber != 4 {
				t.Error("It should be 4")
			}
			atomic.AddInt64(&updateCalled, 1)
		},
	}
	segmentMockStorage := storageMock.MockSegmentStorage{}
	telemetryMockStorage := storageMock.MockTelemetryStorage{
		RecordSyncLatencyCall:    func(resource int, latency time.Duration) {},
		RecordSuccessfulSyncCall: func(resource int, when time.Time) {},
	}
	hcMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {},
	}

	workers := Workers{
		SplitUpdater:   split.NewSplitUpdater(splitMockStorage, splitAPI.SplitFetcher, logger, telemetryMockStorage, hcMonitorMock, flagsets.NewFlagSetFilter(nil)),
		SegmentUpdater: segment.NewSegmentUpdater(splitMockStorage, segmentMockStorage, splitAPI.SegmentFetcher, logger, telemetryMockStorage, hcMonitorMock),
	}
	splitTasks := SplitTasks{
		SegmentSyncTask: tasks.NewFetchSegmentsTask(workers.SegmentUpdater, 1, 1, 500, logger, hcMonitorMock),
		SplitSyncTask:   tasks.NewFetchSplitsTask(workers.SplitUpdater, 1, logger),
	}
	syncForTest := NewSynchronizer(conf.AdvancedConfig{}, splitTasks, workers, logger, nil)

	splitQueue := make(chan dtos.SplitChangeUpdate, 5000)
	splitWorker, _ := push.NewSplitUpdateWorker(splitQueue, syncForTest, logger)
	splitWorker.Start()

	splitQueue <- *dtos.NewSplitChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "some"), 4),
		common.Int64Ref(2), nil,
	)

	time.Sleep(300 * time.Millisecond)
	if !splitWorker.IsRunning() {
		t.Error("It should be running")
	}

	if c := atomic.LoadInt64(&splitFetchCalled); c != 1 {
		t.Error("should have been called once. got: ", c)
	}
	if u := atomic.LoadInt64(&updateCalled); u != 1 {
		t.Error("should have been called once. got: ", u)
	}
}

func TestSplitUpdateWorkerFFPcnDifferentStorageCN(t *testing.T) {
	var splitFetchCalled int64
	var updateCalled int64
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitAPI := api.SplitAPI{
		SplitFetcher: httpMocks.MockSplitFetcher{
			FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
				atomic.AddInt64(&splitFetchCalled, 1)
				return &dtos.SplitChangesDTO{
					Till:   2,
					Since:  2,
					Splits: []dtos.SplitDTO{},
				}, nil
			},
		},
	}
	splitMockStorage := storageMock.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return 1, nil
		},
		UpdateCall: func(toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO, changeNumber int64) {
			if changeNumber != 2 {
				t.Error("It should be 2")
			}
			atomic.AddInt64(&updateCalled, 1)
		},
	}
	segmentMockStorage := storageMock.MockSegmentStorage{}
	telemetryMockStorage := storageMock.MockTelemetryStorage{
		RecordSyncLatencyCall:    func(resource int, latency time.Duration) {},
		RecordSuccessfulSyncCall: func(resource int, when time.Time) {},
	}
	hcMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {},
	}

	workers := Workers{
		SplitUpdater:   split.NewSplitUpdater(splitMockStorage, splitAPI.SplitFetcher, logger, telemetryMockStorage, hcMonitorMock, flagsets.NewFlagSetFilter(nil)),
		SegmentUpdater: segment.NewSegmentUpdater(splitMockStorage, segmentMockStorage, splitAPI.SegmentFetcher, logger, telemetryMockStorage, hcMonitorMock),
	}
	splitTasks := SplitTasks{
		SegmentSyncTask: tasks.NewFetchSegmentsTask(workers.SegmentUpdater, 1, 1, 500, logger, hcMonitorMock),
		SplitSyncTask:   tasks.NewFetchSplitsTask(workers.SplitUpdater, 1, logger),
	}
	syncForTest := NewSynchronizer(conf.AdvancedConfig{}, splitTasks, workers, logger, nil)

	splitQueue := make(chan dtos.SplitChangeUpdate, 5000)
	splitWorker, _ := push.NewSplitUpdateWorker(splitQueue, syncForTest, logger)
	splitWorker.Start()

	featureFlag := dtos.SplitDTO{ChangeNumber: 4, Status: split.Active}
	splitQueue <- *dtos.NewSplitChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "some"), 5),
		common.Int64Ref(2), &featureFlag,
	)

	time.Sleep(300 * time.Millisecond)
	if !splitWorker.IsRunning() {
		t.Error("It should be running")
	}

	if c := atomic.LoadInt64(&splitFetchCalled); c != 1 {
		t.Error("should have been called once. got: ", c)
	}
	if u := atomic.LoadInt64(&updateCalled); u != 1 {
		t.Error("should have been called once. got: ", u)
	}
}

func TestLocalKill(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitAPI := api.SplitAPI{}
	splitMockStorage := storageMock.MockSplitStorage{
		KillLocallyCall: func(splitName, defaultTreatment string, changeNumber int64) {
			if splitName != "split" {
				t.Error("Wrong splitName")
			}
			if defaultTreatment != "default_treatment" {
				t.Error("Wrong defaultTreatment")
			}
			if changeNumber != 123456789 {
				t.Error("Wrong changeNumber")
			}
		},
	}
	workers := Workers{
		SplitUpdater: split.NewSplitUpdater(splitMockStorage, splitAPI.SplitFetcher, logger, storageMock.MockTelemetryStorage{}, hcMock.MockApplicationMonitor{}, flagsets.NewFlagSetFilter(nil)),
	}
	splitTasks := SplitTasks{
		SplitSyncTask: tasks.NewFetchSplitsTask(workers.SplitUpdater, 1, logger),
	}
	syncForTest := NewSynchronizer(conf.AdvancedConfig{}, splitTasks, workers, logger, nil)
	syncForTest.LocalKill("split", "default_treatment", 123456789)
}

func TestSplitUpdateWithReferencedSegments(t *testing.T) {
	var ffUpdateCalled int64
	var segmentUpdateCalled int64
	var segmentFetchCalled int64
	var recordUpdateCall int64
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitAPI := api.SplitAPI{SegmentFetcher: httpMocks.MockSegmentFetcher{
		FetchCall: func(name string, fetchOptions *service.SegmentRequestParams) (*dtos.SegmentChangesDTO, error) {
			atomic.AddInt64(&segmentFetchCalled, 1)
			if name != "segment1" {
				t.Error("Wrong name")
			}
			validReqParams(t, fetchOptions)
			return &dtos.SegmentChangesDTO{
				Name:    name,
				Added:   []string{"some"},
				Removed: []string{},
				Since:   123,
				Till:    123,
			}, nil
		},
	}}
	splitMockStorage := storageMock.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return 1, nil
		},
		UpdateCall: func(toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO, changeNumber int64) {
			if len(toAdd) != 1 {
				t.Error("toAdd should have one feature flag")
			}
			if len(toRemove) != 0 {
				t.Error("toRemove should be empty")
			}
			atomic.AddInt64(&ffUpdateCalled, 1)
		},
		SegmentNamesCall: func() *set.ThreadUnsafeSet { return set.NewSet("segment1") },
	}
	segmentMockStorage := storageMock.MockSegmentStorage{
		ChangeNumberCall: func(segmentName string) (int64, error) {
			if segmentName != "segment1" {
				t.Error("the segment name should be segment1")
			}
			return -1, nil
		},
		UpdateCall: func(name string, toAdd *set.ThreadUnsafeSet, toRemove *set.ThreadUnsafeSet, changeNumber int64) error {
			atomic.AddInt64(&segmentUpdateCalled, 1)
			if name != "segment1" {
				t.Error("Wrong name")
			}
			return nil
		},
	}
	telemetryMockStorage := storageMock.MockTelemetryStorage{
		RecordUpdatesFromSSECall: func(updateType int) {
			atomic.AddInt64(&recordUpdateCall, 1)
		},
		RecordSuccessfulSyncCall: func(resource int, time time.Time) {},
		RecordSyncLatencyCall:    func(resource int, latency time.Duration) {},
	}
	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {},
	}

	workers := Workers{
		SplitUpdater:      split.NewSplitUpdater(splitMockStorage, splitAPI.SplitFetcher, logger, telemetryMockStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil)),
		SegmentUpdater:    segment.NewSegmentUpdater(splitMockStorage, segmentMockStorage, splitAPI.SegmentFetcher, logger, telemetryMockStorage, appMonitorMock),
		EventRecorder:     event.NewEventRecorderSingle(storageMock.MockEventStorage{}, splitAPI.EventRecorder, logger, dtos.Metadata{}, telemetryMockStorage),
		TelemetryRecorder: telemetry.NewTelemetrySynchronizer(telemetryMockStorage, nil, nil, nil, nil, dtos.Metadata{}, telemetryMockStorage),
	}
	splitTasks := SplitTasks{
		SegmentSyncTask: tasks.NewFetchSegmentsTask(workers.SegmentUpdater, 10, 5, 50, logger, appMonitorMock),
	}
	syncForTest := NewSynchronizer(conf.AdvancedConfig{}, splitTasks, workers, logger, nil)

	splitQueue := make(chan dtos.SplitChangeUpdate, 5000)
	splitWorker, _ := push.NewSplitUpdateWorker(splitQueue, syncForTest, logger)
	splitWorker.Start()

	featureFlag := dtos.SplitDTO{Name: "ff1", ChangeNumber: 2, Status: split.Active, Conditions: []dtos.ConditionDTO{{MatcherGroup: dtos.MatcherGroupDTO{Matchers: []dtos.MatcherDTO{
		{MatcherType: "IN_SEGMENT", UserDefinedSegment: &dtos.UserDefinedSegmentMatcherDataDTO{SegmentName: "segment1"}}}}}}}

	splitQueue <- *dtos.NewSplitChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "some"), 2),
		common.Int64Ref(1), &featureFlag,
	)

	time.Sleep(300 * time.Millisecond)

	if u := atomic.LoadInt64(&ffUpdateCalled); u != 1 {
		t.Error("should haven been called. got: ", u)
	}
	if s := atomic.LoadInt64(&segmentFetchCalled); s != 1 {
		t.Error("should haven been called. got: ", s)
	}
	if s := atomic.LoadInt64(&segmentUpdateCalled); s != 1 {
		t.Error("should haven been called. got: ", s)
	}
	if r := atomic.LoadInt64(&recordUpdateCall); r != 1 {
		t.Error("should haven been called. got: ", r)
	}
}

// Large Segment test cases
func TestSyncAllWithLargeSegmentLazyLoad(t *testing.T) {
	var segmentCount int64
	segmentUpdater := syncMocks.MockSegmentUpdater{
		SynchronizeSegmentsCall: func() (map[string]segment.UpdateResult, error) {
			atomic.AddInt64(&segmentCount, 1)
			var toReturn map[string]segment.UpdateResult
			return toReturn, nil
		},
	}
	var splitCount int64
	splitUpdater := syncMocks.MockSplitUpdater{
		SynchronizeSplitsCall: func(till *int64) (*split.UpdateResult, error) {
			atomic.AddInt64(&splitCount, 1)
			return &split.UpdateResult{}, nil
		},
	}

	var lsCount int64
	lsUpdater := syncMocks.MockLargeSegmentUpdater{
		SynchronizeLargeSegmentsCall: func() error {
			atomic.AddInt64(&lsCount, 1)
			return nil
		},
	}

	// Workers
	workers := Workers{
		SegmentUpdater:      segmentUpdater,
		SplitUpdater:        splitUpdater,
		LargeSegmentUpdater: lsUpdater,
	}

	// Tasks
	splitTasks := SplitTasks{}

	cfn := conf.AdvancedConfig{
		LargeSegmentLazyLoad: true,
	}
	sync := NewSynchronizer(cfn, splitTasks, workers, logging.NewLogger(&logging.LoggerOptions{}), nil)
	sync.SyncAll()

	if atomic.LoadInt64(&segmentCount) != 1 {
		t.Error("segmentCount should be 1. Acutual: ", segmentCount)
	}
	if atomic.LoadInt64(&splitCount) != 1 {
		t.Error("splitCount should be 1. Acutual: ", splitCount)
	}
	if atomic.LoadInt64(&lsCount) != 0 {
		t.Error("lsCount should be 0. Acutual: ", lsCount)
	}
}

func TestSyncAllWithLargeSegmentLazyLoadFalse(t *testing.T) {
	var segmentCount int64
	segmentUpdater := syncMocks.MockSegmentUpdater{
		SynchronizeSegmentsCall: func() (map[string]segment.UpdateResult, error) {
			atomic.AddInt64(&segmentCount, 1)
			var toReturn map[string]segment.UpdateResult
			return toReturn, nil
		},
	}
	var splitCount int64
	splitUpdater := syncMocks.MockSplitUpdater{
		SynchronizeSplitsCall: func(till *int64) (*split.UpdateResult, error) {
			atomic.AddInt64(&splitCount, 1)
			return &split.UpdateResult{}, nil
		},
	}

	var lsCount int64
	lsUpdater := syncMocks.MockLargeSegmentUpdater{
		SynchronizeLargeSegmentsCall: func() error {
			atomic.AddInt64(&lsCount, 1)
			return nil
		},
	}

	// Workers
	workers := Workers{
		SegmentUpdater:      segmentUpdater,
		SplitUpdater:        splitUpdater,
		LargeSegmentUpdater: lsUpdater,
	}

	// Tasks
	splitTasks := SplitTasks{}

	cfn := conf.AdvancedConfig{
		LargeSegmentLazyLoad: false,
	}
	sync := NewSynchronizer(cfn, splitTasks, workers, logging.NewLogger(&logging.LoggerOptions{}), nil)
	sync.SyncAll()

	if atomic.LoadInt64(&segmentCount) != 1 {
		t.Error("segmentCount should be 1. Acutual: ", segmentCount)
	}
	if atomic.LoadInt64(&splitCount) != 1 {
		t.Error("splitCount should be 1. Acutual: ", splitCount)
	}
	if atomic.LoadInt64(&lsCount) != 1 {
		t.Error("lsCount should be 1. Acutual: ", lsCount)
	}
}

func TestSynchronizeLargeSegment(t *testing.T) {
	lsName := "ls_test"
	// Workers
	workers := Workers{
		LargeSegmentUpdater: syncMocks.MockLargeSegmentUpdater{
			SynchronizeLargeSegmentCall: func(name string, till *int64) error {
				if name != lsName {
					return fmt.Errorf("wrong large segment name")
				}

				return nil
			},
		},
	}
	splitTasks := SplitTasks{}
	sync := NewSynchronizer(conf.AdvancedConfig{}, splitTasks, workers, logging.NewLogger(&logging.LoggerOptions{}), nil)
	err := sync.SynchronizeLargeSegment(lsName, nil)
	if err != nil {
		t.Error("Error should be nil")
	}
}

func TestStartAndStopFetchingWithLargeSegmentTask(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	advanced := conf.AdvancedConfig{
		SegmentQueueSize:      50,
		SegmentWorkers:        5,
		LargeSegmentQueueSize: 10,
		LargeSegmentWorkers:   5,
	}
	var splitFetchCalled int64
	var segmentFetchCalled int64
	var lsFetchCalled int64
	workers := Workers{
		LargeSegmentUpdater: syncMocks.MockLargeSegmentUpdater{
			SynchronizeLargeSegmentCall: func(name string, till *int64) error {
				atomic.AddInt64(&lsFetchCalled, 1)
				return nil
			},
		},
		SplitUpdater: syncMocks.MockSplitUpdater{
			SynchronizeSplitsCall: func(till *int64) (*split.UpdateResult, error) {
				atomic.AddInt64(&splitFetchCalled, 1)
				return &split.UpdateResult{}, nil
			},
		},
		SegmentUpdater: syncMocks.MockSegmentUpdater{
			SynchronizeSegmentCall: func(name string, till *int64) (*segment.UpdateResult, error) {
				atomic.AddInt64(&segmentFetchCalled, 1)
				return &segment.UpdateResult{}, nil
			},
			SegmentNamesCall: func() []interface{} {
				return set.NewSet("segment1", "segment2").List()
			},
		},
	}
	splitMockStorage := storageMock.MockSplitStorage{
		LargeSegmentNamesCall: func() *set.ThreadUnsafeSet {
			return set.NewSet("ls1", "ls2", "ls3")
		},
	}
	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {},
	}
	splitTasks := SplitTasks{
		SegmentSyncTask:      tasks.NewFetchSegmentsTask(workers.SegmentUpdater, 1, advanced.SegmentWorkers, advanced.SegmentQueueSize, logger, appMonitorMock),
		SplitSyncTask:        tasks.NewFetchSplitsTask(workers.SplitUpdater, 1, logger),
		LargeSegmentSyncTask: tasks.NewFetchLargeSegmentsTask(workers.LargeSegmentUpdater, splitMockStorage, 1, advanced.LargeSegmentWorkers, advanced.LargeSegmentQueueSize, logger),
	}
	sync := NewSynchronizer(conf.AdvancedConfig{}, splitTasks, workers, logging.NewLogger(&logging.LoggerOptions{}), nil)

	sync.StartPeriodicFetching()
	time.Sleep(time.Millisecond * 2200)

	if c := atomic.LoadInt64(&splitFetchCalled); c < 2 {
		t.Error("splitFetchCalled should be called 2. Actual: ", c)
	}
	if c := atomic.LoadInt64(&segmentFetchCalled); c < 2 {
		t.Error("segmentFetchCalled should be called 2. Actual: ", c)
	}
	if c := atomic.LoadInt64(&lsFetchCalled); c < 1 {
		t.Error("lsFetchCalled should be called at least 1. Actual: ", c)
	}
	sync.StopPeriodicFetching()
}
