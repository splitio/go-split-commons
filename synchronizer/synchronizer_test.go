package synchronizer

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service"
	httpMocks "github.com/splitio/go-split-commons/service/mocks"
	storageMock "github.com/splitio/go-split-commons/storage/mocks"
	"github.com/splitio/go-split-commons/synchronizer/worker"
	"github.com/splitio/go-split-commons/synchronizer/worker/event"
	"github.com/splitio/go-split-commons/synchronizer/worker/impression"
	"github.com/splitio/go-toolkit/datastructures/set"
	"github.com/splitio/go-toolkit/logging"
)

func TestSyncAllErrorSplits(t *testing.T) {
	var splitFetchCalled int64
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitAPI := service.SplitAPI{
		SplitFetcher: httpMocks.MockSplitFetcher{
			FetchCall: func(changeNumber int64) (*dtos.SplitChangesDTO, error) {
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
	workers := Workers{
		SplitFetcher:       worker.NewSplitFetcher(splitMockStorage, splitAPI.SplitFetcher, storageMock.MockMetricStorage{}, logger),
		SegmentFetcher:     worker.NewSegmentFetcher(splitMockStorage, storageMock.MockSegmentStorage{}, splitAPI.SegmentFetcher, storageMock.MockMetricStorage{}, logger),
		EventRecorder:      event.NewEventRecorderSingle(storageMock.MockEventStorage{}, splitAPI.EventRecorder, storageMock.MockMetricStorage{}, logger, dtos.Metadata{}),
		ImpressionRecorder: impression.NewRecorderSingle(storageMock.MockImpressionStorage{}, splitAPI.ImpressionRecorder, storageMock.MockMetricStorage{}, logger, dtos.Metadata{}),
		TelemetryRecorder:  worker.NewMetricRecorder(storageMock.MockMetricStorage{}, splitAPI.MetricRecorder, dtos.Metadata{}),
	}
	syncForTest := NewSynchronizer(
		conf.TaskPeriods{CounterSync: 10, EventsSync: 10, GaugeSync: 10, ImpressionSync: 10, LatencySync: 10, SegmentSync: 10, SplitSync: 10},
		conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100, SegmentQueueSize: 50, SegmentWorkers: 5},
		workers,
		logger,
		nil,
	)
	err := syncForTest.SyncAll()
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
			FetchCall: func(changeNumber int64) (*dtos.SplitChangesDTO, error) {
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
			FetchCall: func(name string, changeNumber int64) (*dtos.SegmentChangesDTO, error) {
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
	workers := Workers{
		SplitFetcher:       worker.NewSplitFetcher(splitMockStorage, splitAPI.SplitFetcher, metricMockStorage, logger),
		SegmentFetcher:     worker.NewSegmentFetcher(splitMockStorage, segmentMockStorage, splitAPI.SegmentFetcher, metricMockStorage, logger),
		EventRecorder:      event.NewEventRecorderSingle(storageMock.MockEventStorage{}, splitAPI.EventRecorder, metricMockStorage, logger, dtos.Metadata{}),
		ImpressionRecorder: impression.NewRecorderSingle(storageMock.MockImpressionStorage{}, splitAPI.ImpressionRecorder, metricMockStorage, logger, dtos.Metadata{}),
		TelemetryRecorder:  worker.NewMetricRecorder(metricMockStorage, splitAPI.MetricRecorder, dtos.Metadata{}),
	}
	syncForTest := NewSynchronizer(
		conf.TaskPeriods{CounterSync: 10, EventsSync: 10, GaugeSync: 10, ImpressionSync: 10, LatencySync: 10, SegmentSync: 10, SplitSync: 10},
		conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100, SegmentQueueSize: 50, SegmentWorkers: 5},
		workers,
		logger,
		nil,
	)
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
}

func TestSyncAllOk(t *testing.T) {
	var splitFetchCalled int64
	var segmentFetchCalled int64
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}
	mockedSplit2 := dtos.SplitDTO{Name: "split2", Killed: true, Status: "ACTIVE", TrafficTypeName: "two"}
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitAPI := service.SplitAPI{
		SplitFetcher: httpMocks.MockSplitFetcher{
			FetchCall: func(changeNumber int64) (*dtos.SplitChangesDTO, error) {
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
			FetchCall: func(name string, changeNumber int64) (*dtos.SegmentChangesDTO, error) {
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
	workers := Workers{
		SplitFetcher:       worker.NewSplitFetcher(splitMockStorage, splitAPI.SplitFetcher, metricMockStorage, logger),
		SegmentFetcher:     worker.NewSegmentFetcher(splitMockStorage, segmentMockStorage, splitAPI.SegmentFetcher, metricMockStorage, logger),
		EventRecorder:      event.NewEventRecorderSingle(storageMock.MockEventStorage{}, splitAPI.EventRecorder, metricMockStorage, logger, dtos.Metadata{}),
		ImpressionRecorder: impression.NewRecorderSingle(storageMock.MockImpressionStorage{}, splitAPI.ImpressionRecorder, metricMockStorage, logger, dtos.Metadata{}),
		TelemetryRecorder:  worker.NewMetricRecorder(metricMockStorage, splitAPI.MetricRecorder, dtos.Metadata{}),
	}
	syncForTest := NewSynchronizer(
		conf.TaskPeriods{CounterSync: 10, EventsSync: 10, GaugeSync: 10, ImpressionSync: 10, LatencySync: 10, SegmentSync: 10, SplitSync: 10},
		conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100, SegmentQueueSize: 50, SegmentWorkers: 5},
		workers,
		logger,
		nil,
	)
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
}

func TestPeriodicFetching(t *testing.T) {
	var splitFetchCalled int64
	var segmentFetchCalled int64
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}
	mockedSplit2 := dtos.SplitDTO{Name: "split2", Killed: true, Status: "ACTIVE", TrafficTypeName: "two"}
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitAPI := service.SplitAPI{
		SplitFetcher: httpMocks.MockSplitFetcher{
			FetchCall: func(changeNumber int64) (*dtos.SplitChangesDTO, error) {
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
			FetchCall: func(name string, changeNumber int64) (*dtos.SegmentChangesDTO, error) {
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
	workers := Workers{
		SplitFetcher:       worker.NewSplitFetcher(splitMockStorage, splitAPI.SplitFetcher, metricMockStorage, logger),
		SegmentFetcher:     worker.NewSegmentFetcher(splitMockStorage, segmentMockStorage, splitAPI.SegmentFetcher, metricMockStorage, logger),
		EventRecorder:      event.NewEventRecorderSingle(storageMock.MockEventStorage{}, splitAPI.EventRecorder, metricMockStorage, logger, dtos.Metadata{}),
		ImpressionRecorder: impression.NewRecorderSingle(storageMock.MockImpressionStorage{}, splitAPI.ImpressionRecorder, metricMockStorage, logger, dtos.Metadata{}),
		TelemetryRecorder:  worker.NewMetricRecorder(metricMockStorage, splitAPI.MetricRecorder, dtos.Metadata{}),
	}
	syncForTest := NewSynchronizer(
		conf.TaskPeriods{CounterSync: 100, EventsSync: 100, GaugeSync: 100, ImpressionSync: 100, LatencySync: 100, SegmentSync: 10, SplitSync: 2},
		conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100, SegmentQueueSize: 50, SegmentWorkers: 5},
		workers,
		logger,
		nil,
	)
	syncForTest.StartPeriodicFetching()
	time.Sleep(time.Millisecond * 2200)
	if atomic.LoadInt64(&splitFetchCalled) != 2 {
		t.Error("It should be called twice")
	}
	if atomic.LoadInt64(&segmentFetchCalled) != 2 {
		t.Error("It should be called twice")
	}
	syncForTest.StopPeriodicFetching()
	time.Sleep(time.Second * 3)
	if atomic.LoadInt64(&splitFetchCalled) != 2 {
		t.Error("It should be called twice")
	}
	if atomic.LoadInt64(&segmentFetchCalled) != 2 {
		t.Error("It should be called twice")
	}
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
			RecordCall: func(impressions []dtos.Impression, metadata dtos.Metadata) error {
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
	workers := Workers{
		SplitFetcher:       worker.NewSplitFetcher(storageMock.MockSplitStorage{}, splitAPI.SplitFetcher, metricMockStorage, logger),
		SegmentFetcher:     worker.NewSegmentFetcher(storageMock.MockSplitStorage{}, storageMock.MockSegmentStorage{}, splitAPI.SegmentFetcher, metricMockStorage, logger),
		EventRecorder:      event.NewEventRecorderSingle(eventMockStorage, splitAPI.EventRecorder, metricMockStorage, logger, dtos.Metadata{}),
		ImpressionRecorder: impression.NewRecorderSingle(impressionMockStorage, splitAPI.ImpressionRecorder, metricMockStorage, logger, dtos.Metadata{}),
		TelemetryRecorder:  worker.NewMetricRecorder(metricMockStorage, splitAPI.MetricRecorder, dtos.Metadata{}),
	}
	syncForTest := NewSynchronizer(
		conf.TaskPeriods{CounterSync: 10, EventsSync: 10, GaugeSync: 10, ImpressionSync: 10, LatencySync: 10, SegmentSync: 100, SplitSync: 100},
		conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100, SegmentQueueSize: 50, SegmentWorkers: 5},
		workers,
		logger,
		nil,
	)
	syncForTest.StartPeriodicDataRecording()
	time.Sleep(time.Second * 1)
	if atomic.LoadInt64(&impressionsCalled) != 1 {
		t.Error("It should be called once")
	}
	if atomic.LoadInt64(&eventsCalled) != 1 {
		t.Error("It should be called once")
	}
	if atomic.LoadInt64(&countersCalled) != 1 {
		t.Error("It should be called once")
	}
	if atomic.LoadInt64(&gaugesCalled) != 1 {
		t.Error("It should be called once")
	}
	if atomic.LoadInt64(&latenciesCalled) != 1 {
		t.Error("It should be called once")
	}
	syncForTest.StopPeriodicDataRecording()
	time.Sleep(time.Second * 1)
	if atomic.LoadInt64(&impressionsCalled) != 3 {
		t.Error("It should be called three times")
	}
	if atomic.LoadInt64(&eventsCalled) != 4 {
		t.Error("It should be called fourth times")
	}
	if atomic.LoadInt64(&countersCalled) != 2 {
		t.Error("It should be called twice")
	}
	if atomic.LoadInt64(&gaugesCalled) != 2 {
		t.Error("It should be called twice")
	}
	if atomic.LoadInt64(&latenciesCalled) != 2 {
		t.Error("It should be called twice")
	}
}
