package synchronizer

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/provisional"
	"github.com/splitio/go-split-commons/service"
	httpMocks "github.com/splitio/go-split-commons/service/mocks"
	"github.com/splitio/go-split-commons/storage"
	storageMock "github.com/splitio/go-split-commons/storage/mocks"
	"github.com/splitio/go-split-commons/synchronizer/worker/event"
	"github.com/splitio/go-split-commons/synchronizer/worker/impression"
	"github.com/splitio/go-split-commons/synchronizer/worker/impressionscount"
	"github.com/splitio/go-split-commons/synchronizer/worker/metric"
	"github.com/splitio/go-split-commons/synchronizer/worker/segment"
	"github.com/splitio/go-split-commons/synchronizer/worker/split"
	"github.com/splitio/go-split-commons/tasks"
	"github.com/splitio/go-toolkit/datastructures/set"
	"github.com/splitio/go-toolkit/logging"
)

func TestSyncAllErrorSplits(t *testing.T) {
	var splitFetchCalled int64
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitAPI := service.SplitAPI{
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
	advanced := conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100, SegmentQueueSize: 50, SegmentWorkers: 5}
	metricTestWrapper := storage.NewMetricWrapper(storageMock.MockMetricStorage{}, nil, logger)
	workers := Workers{
		SplitFetcher:       split.NewSplitFetcher(splitMockStorage, splitAPI.SplitFetcher, metricTestWrapper, logger),
		SegmentFetcher:     segment.NewSegmentFetcher(splitMockStorage, storageMock.MockSegmentStorage{}, splitAPI.SegmentFetcher, metricTestWrapper, logger),
		EventRecorder:      event.NewEventRecorderSingle(storageMock.MockEventStorage{}, splitAPI.EventRecorder, metricTestWrapper, logger, dtos.Metadata{}),
		ImpressionRecorder: impression.NewRecorderSingle(storageMock.MockImpressionStorage{}, splitAPI.ImpressionRecorder, metricTestWrapper, logger, dtos.Metadata{}, conf.ManagerConfig{ImpressionsMode: conf.ImpressionsModeDebug}),
		TelemetryRecorder:  metric.NewRecorderSingle(storageMock.MockMetricStorage{}, splitAPI.MetricRecorder, dtos.Metadata{}),
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
	splitAPI := service.SplitAPI{
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
	metricMockStorage := storageMock.MockMetricStorage{
		IncCounterCall: func(key string) {
			if key != "splitChangeFetcher.status.200" && key != "backend::request.ok" {
				t.Error("Unexpected counter key to increase")
			}
		},
		IncLatencyCall: func(metricName string, index int) {
			if metricName != "splitChangeFetcher.time" && metricName != "backend::/api/splitChanges" {
				t.Error("Unexpected latency key to track")
			}
		},
	}
	advanced := conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100, SegmentQueueSize: 50, SegmentWorkers: 5}
	metricTestWrapper := storage.NewMetricWrapper(metricMockStorage, nil, logger)
	workers := Workers{
		SplitFetcher:       split.NewSplitFetcher(splitMockStorage, splitAPI.SplitFetcher, metricTestWrapper, logger),
		SegmentFetcher:     segment.NewSegmentFetcher(splitMockStorage, segmentMockStorage, splitAPI.SegmentFetcher, metricTestWrapper, logger),
		EventRecorder:      event.NewEventRecorderSingle(storageMock.MockEventStorage{}, splitAPI.EventRecorder, metricTestWrapper, logger, dtos.Metadata{}),
		ImpressionRecorder: impression.NewRecorderSingle(storageMock.MockImpressionStorage{}, splitAPI.ImpressionRecorder, metricTestWrapper, logger, dtos.Metadata{}, conf.ManagerConfig{ImpressionsMode: conf.ImpressionsModeDebug}),
		TelemetryRecorder:  metric.NewRecorderSingle(metricMockStorage, splitAPI.MetricRecorder, dtos.Metadata{}),
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
	splitAPI := service.SplitAPI{
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
	metricMockStorage := storageMock.MockMetricStorage{
		IncCounterCall: func(key string) {
			if key != "splitChangeFetcher.status.200" && key != "backend::request.ok" && key != "segmentChangeFetcher.status.200" {
				t.Error("Unexpected counter key to increase")
			}
		},
		IncLatencyCall: func(metricName string, index int) {
			if metricName != "splitChangeFetcher.time" && metricName != "backend::/api/splitChanges" && metricName != "segmentChangeFetcher.time" && metricName != "backend::/api/segmentChanges" {
				t.Error("Unexpected latency key to track")
			}
		},
	}
	advanced := conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100, SegmentQueueSize: 50, SegmentWorkers: 5}
	metricTestWrapper := storage.NewMetricWrapper(metricMockStorage, nil, logger)
	workers := Workers{
		SplitFetcher:       split.NewSplitFetcher(splitMockStorage, splitAPI.SplitFetcher, metricTestWrapper, logger),
		SegmentFetcher:     segment.NewSegmentFetcher(splitMockStorage, segmentMockStorage, splitAPI.SegmentFetcher, metricTestWrapper, logger),
		EventRecorder:      event.NewEventRecorderSingle(storageMock.MockEventStorage{}, splitAPI.EventRecorder, metricTestWrapper, logger, dtos.Metadata{}),
		ImpressionRecorder: impression.NewRecorderSingle(storageMock.MockImpressionStorage{}, splitAPI.ImpressionRecorder, metricTestWrapper, logger, dtos.Metadata{}, conf.ManagerConfig{ImpressionsMode: conf.ImpressionsModeDebug}),
		TelemetryRecorder:  metric.NewRecorderSingle(metricMockStorage, splitAPI.MetricRecorder, dtos.Metadata{}),
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
	splitAPI := service.SplitAPI{
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
	metricMockStorage := storageMock.MockMetricStorage{
		IncCounterCall: func(key string) {
			if key != "splitChangeFetcher.status.200" && key != "backend::request.ok" && key != "segmentChangeFetcher.status.200" {
				t.Error("Unexpected counter key to increase")
			}
		},
		IncLatencyCall: func(metricName string, index int) {
			if metricName != "splitChangeFetcher.time" && metricName != "backend::/api/splitChanges" && metricName != "segmentChangeFetcher.time" && metricName != "backend::/api/segmentChanges" {
				t.Error("Unexpected latency key to track")
			}
		},
	}
	advanced := conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100, SegmentQueueSize: 50, SegmentWorkers: 5}
	metricTestWrapper := storage.NewMetricWrapper(metricMockStorage, nil, logger)
	workers := Workers{
		SplitFetcher:       split.NewSplitFetcher(splitMockStorage, splitAPI.SplitFetcher, metricTestWrapper, logger),
		SegmentFetcher:     segment.NewSegmentFetcher(splitMockStorage, segmentMockStorage, splitAPI.SegmentFetcher, metricTestWrapper, logger),
		EventRecorder:      event.NewEventRecorderSingle(storageMock.MockEventStorage{}, splitAPI.EventRecorder, metricTestWrapper, logger, dtos.Metadata{}),
		ImpressionRecorder: impression.NewRecorderSingle(storageMock.MockImpressionStorage{}, splitAPI.ImpressionRecorder, metricTestWrapper, logger, dtos.Metadata{}, conf.ManagerConfig{ImpressionsMode: conf.ImpressionsModeDebug}),
		TelemetryRecorder:  metric.NewRecorderSingle(metricMockStorage, splitAPI.MetricRecorder, dtos.Metadata{}),
	}
	splitTasks := SplitTasks{
		EventSyncTask:      tasks.NewRecordEventsTask(workers.EventRecorder, advanced.EventsBulkSize, 1, logger),
		ImpressionSyncTask: tasks.NewRecordImpressionsTask(workers.ImpressionRecorder, 1, logger, advanced.ImpressionsBulkSize),
		SegmentSyncTask:    tasks.NewFetchSegmentsTask(workers.SegmentFetcher, 1, advanced.SegmentWorkers, advanced.SegmentQueueSize, logger),
		SplitSyncTask:      tasks.NewFetchSplitsTask(workers.SplitFetcher, 1, logger),
		TelemetrySyncTask:  tasks.NewRecordTelemetryTask(workers.TelemetryRecorder, 1, logger),
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
	var countersCalled int64
	var gaugesCalled int64
	var latenciesCalled int64
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitAPI := service.SplitAPI{
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
		MetricRecorder: httpMocks.MockMetricRecorder{
			RecordCountersCall: func(counters []dtos.CounterDTO, metadata dtos.Metadata) error {
				atomic.AddInt64(&countersCalled, 1)
				if len(counters) != 1 {
					t.Error("Wrong length")
				}
				return nil
			},
			RecordGaugeCall: func(gauge dtos.GaugeDTO, metadata dtos.Metadata) error {
				atomic.AddInt64(&gaugesCalled, 1)
				if gauge.MetricName != "gauge" {
					t.Error("Wrong gauge")
				}
				return nil
			},
			RecordLatenciesCall: func(latencies []dtos.LatenciesDTO, metadata dtos.Metadata) error {
				atomic.AddInt64(&latenciesCalled, 1)
				if len(latencies) != 1 {
					t.Error("Wrong length")
				}
				return nil
			},
		},
	}
	metricMockStorage := storageMock.MockMetricStorage{
		PopCountersCall: func() []dtos.CounterDTO {
			return []dtos.CounterDTO{{MetricName: "counter", Count: 1}}
		},
		PopGaugesCall: func() []dtos.GaugeDTO {
			return []dtos.GaugeDTO{{MetricName: "gauge", Gauge: 1}}
		},
		PopLatenciesCall: func() []dtos.LatenciesDTO {
			return []dtos.LatenciesDTO{{MetricName: "latency", Latencies: []int64{1, 2, 3, 4}}}
		},
		IncCounterCall: func(key string) {},
		IncLatencyCall: func(metricName string, index int) {},
		PutGaugeCall:   func(key string, gauge float64) {},
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
			if impressionsCalled < 3 {
				return false
			}
			return true
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
			if eventsCalled < 4 {
				return false
			}
			return true
		},
	}
	advanced := conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100, SegmentQueueSize: 50, SegmentWorkers: 5}
	metricTestWrapper := storage.NewMetricWrapper(metricMockStorage, nil, logger)
	workers := Workers{
		SplitFetcher:       split.NewSplitFetcher(storageMock.MockSplitStorage{}, splitAPI.SplitFetcher, metricTestWrapper, logger),
		SegmentFetcher:     segment.NewSegmentFetcher(storageMock.MockSplitStorage{}, storageMock.MockSegmentStorage{}, splitAPI.SegmentFetcher, metricTestWrapper, logger),
		EventRecorder:      event.NewEventRecorderSingle(eventMockStorage, splitAPI.EventRecorder, metricTestWrapper, logger, dtos.Metadata{}),
		ImpressionRecorder: impression.NewRecorderSingle(impressionMockStorage, splitAPI.ImpressionRecorder, metricTestWrapper, logger, dtos.Metadata{}, conf.ManagerConfig{ImpressionsMode: conf.ImpressionsModeDebug}),
		TelemetryRecorder:  metric.NewRecorderSingle(metricMockStorage, splitAPI.MetricRecorder, dtos.Metadata{}),
	}
	splitTasks := SplitTasks{
		EventSyncTask:      tasks.NewRecordEventsTask(workers.EventRecorder, advanced.EventsBulkSize, 1, logger),
		ImpressionSyncTask: tasks.NewRecordImpressionsTask(workers.ImpressionRecorder, 1, logger, advanced.ImpressionsBulkSize),
		SegmentSyncTask:    tasks.NewFetchSegmentsTask(workers.SegmentFetcher, 1, advanced.SegmentWorkers, advanced.SegmentQueueSize, logger),
		SplitSyncTask:      tasks.NewFetchSplitsTask(workers.SplitFetcher, 1, logger),
		TelemetrySyncTask:  tasks.NewRecordTelemetryTask(workers.TelemetryRecorder, 1, logger),
	}
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
	if atomic.LoadInt64(&countersCalled) < 1 {
		t.Error("It should be called once")
	}
	if atomic.LoadInt64(&gaugesCalled) < 1 {
		t.Error("It should be called once")
	}
	if atomic.LoadInt64(&latenciesCalled) < 1 {
		t.Error("It should be called once")
	}
	syncForTest.StopPeriodicDataRecording()
	time.Sleep(time.Second * 1)
	if atomic.LoadInt64(&impressionsCalled) < 2 {
		t.Error("It should be called three times")
	}
	if atomic.LoadInt64(&eventsCalled) < 2 {
		t.Error("It should be called fourth times")
	}
}

func TestPeriodicRecordingWithCounter(t *testing.T) {
	var impressionsCalled int64
	var eventsCalled int64
	var countersCalled int64
	var gaugesCalled int64
	var latenciesCalled int64
	var counterCalled int64
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitAPI := service.SplitAPI{
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
			RecordImpressionsCountCall: func(pf dtos.ImpressionsCountDTO, metadata dtos.Metadata) error {
				atomic.AddInt64(&counterCalled, 1)
				if len(pf.PerFeature) != 1 {
					t.Error("Wrong length")
				}
				return nil
			},
		},
		MetricRecorder: httpMocks.MockMetricRecorder{
			RecordCountersCall: func(counters []dtos.CounterDTO, metadata dtos.Metadata) error {
				atomic.AddInt64(&countersCalled, 1)
				if len(counters) != 1 {
					t.Error("Wrong length")
				}
				return nil
			},
			RecordGaugeCall: func(gauge dtos.GaugeDTO, metadata dtos.Metadata) error {
				atomic.AddInt64(&gaugesCalled, 1)
				if gauge.MetricName != "gauge" {
					t.Error("Wrong gauge")
				}
				return nil
			},
			RecordLatenciesCall: func(latencies []dtos.LatenciesDTO, metadata dtos.Metadata) error {
				atomic.AddInt64(&latenciesCalled, 1)
				if len(latencies) != 1 {
					t.Error("Wrong length")
				}
				return nil
			},
		},
	}
	metricMockStorage := storageMock.MockMetricStorage{
		PopCountersCall: func() []dtos.CounterDTO {
			return []dtos.CounterDTO{{MetricName: "counter", Count: 1}}
		},
		PopGaugesCall: func() []dtos.GaugeDTO {
			return []dtos.GaugeDTO{{MetricName: "gauge", Gauge: 1}}
		},
		PopLatenciesCall: func() []dtos.LatenciesDTO {
			return []dtos.LatenciesDTO{{MetricName: "latency", Latencies: []int64{1, 2, 3, 4}}}
		},
		IncCounterCall: func(key string) {},
		IncLatencyCall: func(metricName string, index int) {},
		PutGaugeCall:   func(key string, gauge float64) {},
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
			if impressionsCalled < 3 {
				return false
			}
			return true
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
			if eventsCalled < 4 {
				return false
			}
			return true
		},
	}
	advanced := conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100, SegmentQueueSize: 50, SegmentWorkers: 5}
	metricTestWrapper := storage.NewMetricWrapper(metricMockStorage, nil, logger)
	impCounter := provisional.NewImpressionsCounter()
	impCounter.Inc("some", time.Now().UTC().UnixNano(), 1)
	workers := Workers{
		SplitFetcher:             split.NewSplitFetcher(storageMock.MockSplitStorage{}, splitAPI.SplitFetcher, metricTestWrapper, logger),
		SegmentFetcher:           segment.NewSegmentFetcher(storageMock.MockSplitStorage{}, storageMock.MockSegmentStorage{}, splitAPI.SegmentFetcher, metricTestWrapper, logger),
		EventRecorder:            event.NewEventRecorderSingle(eventMockStorage, splitAPI.EventRecorder, metricTestWrapper, logger, dtos.Metadata{}),
		ImpressionRecorder:       impression.NewRecorderSingle(impressionMockStorage, splitAPI.ImpressionRecorder, metricTestWrapper, logger, dtos.Metadata{}, conf.ManagerConfig{ImpressionsMode: conf.ImpressionsModeDebug}),
		TelemetryRecorder:        metric.NewRecorderSingle(metricMockStorage, splitAPI.MetricRecorder, dtos.Metadata{}),
		ImpressionsCountRecorder: impressionscount.NewRecorderSingle(impCounter, splitAPI.ImpressionRecorder, dtos.Metadata{}, logger),
	}
	splitTasks := SplitTasks{
		EventSyncTask:            tasks.NewRecordEventsTask(workers.EventRecorder, advanced.EventsBulkSize, 1, logger),
		ImpressionSyncTask:       tasks.NewRecordImpressionsTask(workers.ImpressionRecorder, 1, logger, advanced.ImpressionsBulkSize),
		SegmentSyncTask:          tasks.NewFetchSegmentsTask(workers.SegmentFetcher, 1, advanced.SegmentWorkers, advanced.SegmentQueueSize, logger),
		SplitSyncTask:            tasks.NewFetchSplitsTask(workers.SplitFetcher, 1, logger),
		TelemetrySyncTask:        tasks.NewRecordTelemetryTask(workers.TelemetryRecorder, 1, logger),
		ImpressionsCountSyncTask: tasks.NewRecordImpressionsCountTask(workers.ImpressionsCountRecorder, logger),
	}
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
	if atomic.LoadInt64(&countersCalled) < 1 {
		t.Error("It should be called once")
	}
	if atomic.LoadInt64(&gaugesCalled) < 1 {
		t.Error("It should be called once")
	}
	if atomic.LoadInt64(&latenciesCalled) < 1 {
		t.Error("It should be called once")
	}
	if atomic.LoadInt64(&counterCalled) != 0 { // it won't execute until one hour or shutdown's flush
		t.Error("It should be called once")
	}
	impCounter.Inc("some", time.Now().UTC().UnixNano(), 1)
	syncForTest.StopPeriodicDataRecording()
	time.Sleep(time.Second * 1)
	if atomic.LoadInt64(&impressionsCalled) < 3 {
		t.Error("It should be called three times")
	}
	if atomic.LoadInt64(&eventsCalled) < 3 {
		t.Error("It should be called fourth times")
	}
	if atomic.LoadInt64(&countersCalled) < 2 {
		t.Error("It should be called twice")
	}
	if atomic.LoadInt64(&gaugesCalled) < 2 {
		t.Error("It should be called twice")
	}
	if atomic.LoadInt64(&latenciesCalled) < 2 {
		t.Error("It should be called twice")
	}
	if atomic.LoadInt64(&counterCalled) < 1 {
		t.Error("It should be called once")
	}
}
