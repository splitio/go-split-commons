package synchronizer

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v3/conf"
	"github.com/splitio/go-split-commons/v3/dtos"
	"github.com/splitio/go-split-commons/v3/service/api"
	httpMocks "github.com/splitio/go-split-commons/v3/service/mocks"
	storageMock "github.com/splitio/go-split-commons/v3/storage/mocks"
	"github.com/splitio/go-split-commons/v3/synchronizer/worker/event"
	"github.com/splitio/go-split-commons/v3/synchronizer/worker/impression"
	"github.com/splitio/go-split-commons/v3/synchronizer/worker/segment"
	"github.com/splitio/go-split-commons/v3/synchronizer/worker/split"
	"github.com/splitio/go-split-commons/v3/tasks"
	"github.com/splitio/go-split-commons/v3/telemetry"
	"github.com/splitio/go-toolkit/v4/datastructures/set"
	"github.com/splitio/go-toolkit/v4/logging"
)

func TestSyncAllErrorSplits(t *testing.T) {
	var splitFetchCalled int64
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitAPI := api.SplitAPI{
		SplitFetcher: httpMocks.MockSplitFetcher{
			FetchCall: func(changeNumber int64, noCache bool) (*dtos.SplitChangesDTO, error) {
				if !noCache {
					t.Error("no cache should be true")
				}
				atomic.AddInt64(&splitFetchCalled, 1)
				if changeNumber != -1 {
					t.Error("Wrong changenumber passed")
				}
				return nil, errors.New("Some")
			},
		},
	}
	splitMockStorage := storageMock.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return -1, nil
		},
	}
	telemetryStorage := storageMock.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, time int64) {},
	}
	advanced := conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100, SegmentQueueSize: 50, SegmentWorkers: 5}
	workers := Workers{
		SplitFetcher:       split.NewSplitFetcher(splitMockStorage, splitAPI.SplitFetcher, logger),
		SegmentFetcher:     segment.NewSegmentFetcher(splitMockStorage, storageMock.MockSegmentStorage{}, splitAPI.SegmentFetcher, logger),
		EventRecorder:      event.NewEventRecorderSingle(storageMock.MockEventStorage{}, splitAPI.EventRecorder, logger, dtos.Metadata{}, telemetryStorage),
		ImpressionRecorder: impression.NewRecorderSingle(storageMock.MockImpressionStorage{}, splitAPI.ImpressionRecorder, logger, dtos.Metadata{}, conf.ManagerConfig{ImpressionsMode: conf.ImpressionsModeDebug}),
		TelemetryRecorder:  telemetry.NewTelemetrySynchronizer(telemetryStorage, nil, nil, nil, nil, dtos.Metadata{}),
	}
	splitTasks := SplitTasks{
		EventSyncTask:      tasks.NewRecordEventsTask(workers.EventRecorder, advanced.EventsBulkSize, 10, logger),
		ImpressionSyncTask: tasks.NewRecordImpressionsTask(workers.ImpressionRecorder, 10, logger, advanced.ImpressionsBulkSize),
		SegmentSyncTask:    tasks.NewFetchSegmentsTask(workers.SegmentFetcher, 10, advanced.SegmentWorkers, advanced.SegmentQueueSize, logger),
		SplitSyncTask:      tasks.NewFetchSplitsTask(workers.SplitFetcher, 10, logger),
		TelemetrySyncTask:  tasks.NewRecordTelemetryTask(workers.TelemetryRecorder, 10, logger),
	}
	syncForTest := NewSynchronizer(
		advanced,
		splitTasks,
		workers,
		logger,
		nil,
	)
	err := syncForTest.SyncAll(true)
	if err == nil {
		t.Error("It should return error")
	}
	if atomic.LoadInt64(&splitFetchCalled) != 1 {
		t.Error("It should be called once")
	}
}

func TestSyncAllErrorInSegments(t *testing.T) {
	var splitFetchCalled int64
	var segmentFetchCalled int64
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}
	mockedSplit2 := dtos.SplitDTO{Name: "split2", Killed: true, Status: "ACTIVE", TrafficTypeName: "two"}
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitAPI := api.SplitAPI{
		SplitFetcher: httpMocks.MockSplitFetcher{
			FetchCall: func(changeNumber int64, noCache bool) (*dtos.SplitChangesDTO, error) {
				if noCache {
					t.Error("noCache should be false")
				}
				atomic.AddInt64(&splitFetchCalled, 1)
				if changeNumber != -1 {
					t.Error("Wrong changenumber passed")
				}
				return &dtos.SplitChangesDTO{
					Splits: []dtos.SplitDTO{mockedSplit1, mockedSplit2},
					Since:  3,
					Till:   3,
				}, nil
			},
		},
		SegmentFetcher: httpMocks.MockSegmentFetcher{
			FetchCall: func(name string, changeNumber int64, noCache bool) (*dtos.SegmentChangesDTO, error) {
				if noCache {
					t.Error("noCache should be false")
				}
				atomic.AddInt64(&segmentFetchCalled, 1)
				if name != "segment1" && name != "segment2" {
					t.Error("Wrong name")
				}
				return nil, errors.New("some")
			},
		},
	}
	splitMockStorage := storageMock.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return -1, nil
		},
		PutManyCall: func(splits []dtos.SplitDTO, changeNumber int64) {
			if changeNumber != 3 {
				t.Error("Wrong changenumber")
			}
			if len(splits) != 2 {
				t.Error("Wrong length of passed splits")
			}
		},
		SegmentNamesCall: func() *set.ThreadUnsafeSet {
			segmentNames := set.NewSet("segment1", "segment2")
			return segmentNames
		},
	}
	segmentMockStorage := storageMock.MockSegmentStorage{
		ChangeNumberCall: func(segmentName string) (int64, error) {
			return -1, nil
		},
	}
	telemetryStorage := storageMock.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, time int64) {},
	}
	advanced := conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100, SegmentQueueSize: 50, SegmentWorkers: 5}
	workers := Workers{
		SplitFetcher:       split.NewSplitFetcher(splitMockStorage, splitAPI.SplitFetcher, logger),
		SegmentFetcher:     segment.NewSegmentFetcher(splitMockStorage, segmentMockStorage, splitAPI.SegmentFetcher, logger),
		EventRecorder:      event.NewEventRecorderSingle(storageMock.MockEventStorage{}, splitAPI.EventRecorder, logger, dtos.Metadata{}, telemetryStorage),
		ImpressionRecorder: impression.NewRecorderSingle(storageMock.MockImpressionStorage{}, splitAPI.ImpressionRecorder, logger, dtos.Metadata{}, conf.ManagerConfig{ImpressionsMode: conf.ImpressionsModeDebug}),
		TelemetryRecorder:  telemetry.NewTelemetrySynchronizer(telemetryStorage, nil, nil, nil, nil, dtos.Metadata{}),
	}
	splitTasks := SplitTasks{
		EventSyncTask:      tasks.NewRecordEventsTask(workers.EventRecorder, advanced.EventsBulkSize, 10, logger),
		ImpressionSyncTask: tasks.NewRecordImpressionsTask(workers.ImpressionRecorder, 10, logger, advanced.ImpressionsBulkSize),
		SegmentSyncTask:    tasks.NewFetchSegmentsTask(workers.SegmentFetcher, 10, advanced.SegmentWorkers, advanced.SegmentQueueSize, logger),
		SplitSyncTask:      tasks.NewFetchSplitsTask(workers.SplitFetcher, 10, logger),
		TelemetrySyncTask:  tasks.NewRecordTelemetryTask(workers.TelemetryRecorder, 10, logger),
	}
	syncForTest := NewSynchronizer(
		advanced,
		splitTasks,
		workers,
		logger,
		nil,
	)
	err := syncForTest.SyncAll(false)
	if err == nil {
		t.Error("It should return error")
	}
	if atomic.LoadInt64(&splitFetchCalled) != 1 {
		t.Error("It should be called once")
	}
	if atomic.LoadInt64(&segmentFetchCalled) != 2 {
		t.Error("It should be called twice")
	}
}

func TestSyncAllOk(t *testing.T) {
	var splitFetchCalled int64
	var segmentFetchCalled int64
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}
	mockedSplit2 := dtos.SplitDTO{Name: "split2", Killed: true, Status: "ACTIVE", TrafficTypeName: "two"}
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitAPI := api.SplitAPI{
		SplitFetcher: httpMocks.MockSplitFetcher{
			FetchCall: func(changeNumber int64, noCache bool) (*dtos.SplitChangesDTO, error) {
				if !noCache {
					t.Error("noCache should be true")
				}
				atomic.AddInt64(&splitFetchCalled, 1)
				if changeNumber != -1 {
					t.Error("Wrong changenumber passed")
				}
				return &dtos.SplitChangesDTO{
					Splits: []dtos.SplitDTO{mockedSplit1, mockedSplit2},
					Since:  3,
					Till:   3,
				}, nil
			},
		},
		SegmentFetcher: httpMocks.MockSegmentFetcher{
			FetchCall: func(name string, changeNumber int64, noCache bool) (*dtos.SegmentChangesDTO, error) {
				if !noCache {
					t.Error("noCache should be true")
				}
				atomic.AddInt64(&segmentFetchCalled, 1)
				if name != "segment1" && name != "segment2" {
					t.Error("Wrong name")
				}
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
		ChangeNumberCall: func() (int64, error) {
			return -1, nil
		},
		PutManyCall: func(splits []dtos.SplitDTO, changeNumber int64) {
			if changeNumber != 3 {
				t.Error("Wrong changenumber")
			}
			if len(splits) != 2 {
				t.Error("Wrong length of passed splits")
			}
		},
		SegmentNamesCall: func() *set.ThreadUnsafeSet {
			segmentNames := set.NewSet("segment1", "segment2")
			return segmentNames
		},
	}
	segmentMockStorage := storageMock.MockSegmentStorage{
		ChangeNumberCall: func(segmentName string) (int64, error) {
			return -1, nil
		},
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
	telemetryStorage := storageMock.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, time int64) {},
	}
	advanced := conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100, SegmentQueueSize: 50, SegmentWorkers: 5}
	workers := Workers{
		SplitFetcher:       split.NewSplitFetcher(splitMockStorage, splitAPI.SplitFetcher, logger),
		SegmentFetcher:     segment.NewSegmentFetcher(splitMockStorage, segmentMockStorage, splitAPI.SegmentFetcher, logger),
		EventRecorder:      event.NewEventRecorderSingle(storageMock.MockEventStorage{}, splitAPI.EventRecorder, logger, dtos.Metadata{}, telemetryStorage),
		ImpressionRecorder: impression.NewRecorderSingle(storageMock.MockImpressionStorage{}, splitAPI.ImpressionRecorder, logger, dtos.Metadata{}, conf.ManagerConfig{ImpressionsMode: conf.ImpressionsModeDebug}),
		TelemetryRecorder:  telemetry.NewTelemetrySynchronizer(telemetryStorage, nil, nil, nil, nil, dtos.Metadata{}),
	}
	splitTasks := SplitTasks{
		EventSyncTask:      tasks.NewRecordEventsTask(workers.EventRecorder, advanced.EventsBulkSize, 10, logger),
		ImpressionSyncTask: tasks.NewRecordImpressionsTask(workers.ImpressionRecorder, 10, logger, advanced.ImpressionsBulkSize),
		SegmentSyncTask:    tasks.NewFetchSegmentsTask(workers.SegmentFetcher, 10, advanced.SegmentWorkers, advanced.SegmentQueueSize, logger),
		SplitSyncTask:      tasks.NewFetchSplitsTask(workers.SplitFetcher, 10, logger),
		TelemetrySyncTask:  tasks.NewRecordTelemetryTask(workers.TelemetryRecorder, 10, logger),
	}
	syncForTest := NewSynchronizer(
		advanced,
		splitTasks,
		workers,
		logger,
		nil,
	)
	err := syncForTest.SyncAll(true)
	if err != nil {
		t.Error("It should not return error")
	}
	if splitFetchCalled != 1 {
		t.Error("It should be called once")
	}
	if segmentFetchCalled != 2 {
		t.Error("It should be called twice")
	}
}

func TestPeriodicFetching(t *testing.T) {
	var splitFetchCalled int64
	var segmentFetchCalled int64
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}
	mockedSplit2 := dtos.SplitDTO{Name: "split2", Killed: true, Status: "ACTIVE", TrafficTypeName: "two"}
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitAPI := api.SplitAPI{
		SplitFetcher: httpMocks.MockSplitFetcher{
			FetchCall: func(changeNumber int64, noCache bool) (*dtos.SplitChangesDTO, error) {
				if noCache {
					t.Error("noCache should be false")
				}
				atomic.AddInt64(&splitFetchCalled, 1)
				if changeNumber != -1 {
					t.Error("Wrong changenumber passed")
				}
				return &dtos.SplitChangesDTO{
					Splits: []dtos.SplitDTO{mockedSplit1, mockedSplit2},
					Since:  3,
					Till:   3,
				}, nil
			},
		},
		SegmentFetcher: httpMocks.MockSegmentFetcher{
			FetchCall: func(name string, changeNumber int64, noCache bool) (*dtos.SegmentChangesDTO, error) {
				if noCache {
					t.Error("noCache should be false")
				}
				atomic.AddInt64(&segmentFetchCalled, 1)
				if name != "segment1" && name != "segment2" {
					t.Error("Wrong name")
				}
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
		ChangeNumberCall: func() (int64, error) {
			return -1, nil
		},
		PutManyCall: func(splits []dtos.SplitDTO, changeNumber int64) {
			if changeNumber != 3 {
				t.Error("Wrong changenumber")
			}
			if len(splits) != 2 {
				t.Error("Wrong length of passed splits")
			}
		},
		SegmentNamesCall: func() *set.ThreadUnsafeSet {
			segmentNames := set.NewSet("segment1", "segment2")
			return segmentNames
		},
	}
	segmentMockStorage := storageMock.MockSegmentStorage{
		ChangeNumberCall: func(segmentName string) (int64, error) {
			return -1, nil
		},
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
	telemetryStorage := storageMock.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, time int64) {},
	}
	advanced := conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100, SegmentQueueSize: 50, SegmentWorkers: 5}
	workers := Workers{
		SplitFetcher:       split.NewSplitFetcher(splitMockStorage, splitAPI.SplitFetcher, logger),
		SegmentFetcher:     segment.NewSegmentFetcher(splitMockStorage, segmentMockStorage, splitAPI.SegmentFetcher, logger),
		EventRecorder:      event.NewEventRecorderSingle(storageMock.MockEventStorage{}, splitAPI.EventRecorder, logger, dtos.Metadata{}, telemetryStorage),
		ImpressionRecorder: impression.NewRecorderSingle(storageMock.MockImpressionStorage{}, splitAPI.ImpressionRecorder, logger, dtos.Metadata{}, conf.ManagerConfig{ImpressionsMode: conf.ImpressionsModeDebug}),
		TelemetryRecorder:  telemetry.NewTelemetrySynchronizer(telemetryStorage, nil, nil, nil, nil, dtos.Metadata{}),
	}
	splitTasks := SplitTasks{
		EventSyncTask:      tasks.NewRecordEventsTask(workers.EventRecorder, advanced.EventsBulkSize, 1, logger),
		ImpressionSyncTask: tasks.NewRecordImpressionsTask(workers.ImpressionRecorder, 1, logger, advanced.ImpressionsBulkSize),
		SegmentSyncTask:    tasks.NewFetchSegmentsTask(workers.SegmentFetcher, 1, advanced.SegmentWorkers, advanced.SegmentQueueSize, logger),
		SplitSyncTask:      tasks.NewFetchSplitsTask(workers.SplitFetcher, 1, logger),
		TelemetrySyncTask:  tasks.NewRecordTelemetryTask(workers.TelemetryRecorder, 10, logger),
	}
	syncForTest := NewSynchronizer(
		advanced,
		splitTasks,
		workers,
		logger,
		nil,
	)
	syncForTest.StartPeriodicFetching()
	time.Sleep(time.Millisecond * 2200)
	if atomic.LoadInt64(&splitFetchCalled) < 2 {
		t.Error("It should be called twice")
	}
	if atomic.LoadInt64(&segmentFetchCalled) < 2 {
		t.Error("It should be called twice")
	}
	syncForTest.StopPeriodicFetching()
}

func TestPeriodicRecording(t *testing.T) {
	var impressionsCalled int64
	var eventsCalled int64
	var statsCalled int64
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
		EmptyCall: func() bool {
			return impressionsCalled >= 3
		},
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
		EmptyCall: func() bool {
			return eventsCalled >= 4
		},
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
		RecordSuccessfulSyncCall:   func(resource int, time int64) {},
	}
	splitMockStorage := storageMock.MockSplitStorage{
		SplitNamesCall:   func() []string { return []string{} },
		SegmentNamesCall: func() *set.ThreadUnsafeSet { return set.NewSet() },
	}
	segmentMockStorage := storageMock.MockSegmentStorage{
		SegmentKeysCountCall: func() int64 { return 30 },
	}
	advanced := conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100, SegmentQueueSize: 50, SegmentWorkers: 5}
	workers := Workers{
		SplitFetcher:       split.NewSplitFetcher(splitMockStorage, splitAPI.SplitFetcher, logger),
		SegmentFetcher:     segment.NewSegmentFetcher(splitMockStorage, segmentMockStorage, splitAPI.SegmentFetcher, logger),
		EventRecorder:      event.NewEventRecorderSingle(eventMockStorage, splitAPI.EventRecorder, logger, dtos.Metadata{}, telemetryMockStorage),
		ImpressionRecorder: impression.NewRecorderSingle(impressionMockStorage, splitAPI.ImpressionRecorder, logger, dtos.Metadata{}, conf.ManagerConfig{ImpressionsMode: conf.ImpressionsModeDebug}),
		TelemetryRecorder:  telemetry.NewTelemetrySynchronizer(telemetryMockStorage, splitAPI.TelemetryRecorder, splitMockStorage, segmentMockStorage, logger, dtos.Metadata{}),
	}
	splitTasks := SplitTasks{
		EventSyncTask:      tasks.NewRecordEventsTask(workers.EventRecorder, advanced.EventsBulkSize, 1, logger),
		ImpressionSyncTask: tasks.NewRecordImpressionsTask(workers.ImpressionRecorder, 1, logger, advanced.ImpressionsBulkSize),
		SegmentSyncTask:    tasks.NewFetchSegmentsTask(workers.SegmentFetcher, 1, advanced.SegmentWorkers, advanced.SegmentQueueSize, logger),
		SplitSyncTask:      tasks.NewFetchSplitsTask(workers.SplitFetcher, 1, logger),
		TelemetrySyncTask:  tasks.NewRecordTelemetryTask(workers.TelemetryRecorder, 1, logger),
	}
	workers.TelemetryRecorder.SynchronizeStats()
	syncForTest := NewSynchronizer(
		advanced,
		splitTasks,
		workers,
		logger,
		nil,
	)
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
}
