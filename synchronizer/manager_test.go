package synchronizer

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service"
	"github.com/splitio/go-split-commons/service/mocks"
	httpMocks "github.com/splitio/go-split-commons/service/mocks"
	storageMock "github.com/splitio/go-split-commons/storage/mocks"
	"github.com/splitio/go-toolkit/datastructures/set"
	"github.com/splitio/go-toolkit/logging"
)

func TestPolling(t *testing.T) {
	var shouldBeReady int64
	var impressionsCalled int64
	var eventsCalled int64
	var countersCalled int64
	var gaugesCalled int64
	var latenciesCalled int64
	var splitFetchCalled int64
	var segmentFetchCalled int64
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}
	mockedSplit2 := dtos.SplitDTO{Name: "split2", Killed: true, Status: "ACTIVE", TrafficTypeName: "two"}
	advanced := conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100, SegmentQueueSize: 50, SegmentWorkers: 5, StreamingEnabled: false}
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
	syncForTest := NewSynchronizer(
		conf.TaskPeriods{CounterSync: 10, EventsSync: 10, GaugeSync: 10, ImpressionSync: 10, LatencySync: 10, SegmentSync: 10, SplitSync: 10},
		advanced,
		&splitAPI,
		storageMock.MockSplitStorage{
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
		},
		storageMock.MockSegmentStorage{
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
		},
		storageMock.MockMetricStorage{
			PopCountersCall: func() []dtos.CounterDTO {
				return []dtos.CounterDTO{{MetricName: "counter", Count: 1}}
			},
			PopGaugesCall: func() []dtos.GaugeDTO {
				return []dtos.GaugeDTO{{MetricName: "gauge", Gauge: 1}}
			},
			PopLatenciesCall: func() []dtos.LatenciesDTO {
				return []dtos.LatenciesDTO{{MetricName: "latency", Latencies: []int64{1, 2, 3, 4}}}
			},
			IncLatencyCall: func(metricName string, index int) {},
			IncCounterCall: func(key string) {},
			PutGaugeCall:   func(key string, gauge float64) {},
		},
		storageMock.MockImpressionStorage{
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
		},
		storageMock.MockEventStorage{
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
		},
		logger,
		nil,
		&dtos.Metadata{},
	)

	readyChannel := make(chan string, 1)
	managerTest := NewSynchronizerManager(
		syncForTest,
		logger,
		readyChannel,
		advanced,
		mocks.MockAuthClient{
			AuthenticateCall: func() (*dtos.Token, error) {
				return nil, nil
			},
		},
		storageMock.MockSplitStorage{},
	)
	managerTest.Start()

	time.Sleep(time.Second * 1)
	if splitFetchCalled != 2 {
		t.Error("It should be called twice")
		t.Error(splitFetchCalled)
	}
	if segmentFetchCalled != 4 {
		t.Error("It should be called fourth times")
		t.Error(segmentFetchCalled)
	}

	if impressionsCalled != 1 {
		t.Error("It should be called once")
		t.Error(impressionsCalled)
	}
	if eventsCalled != 1 {
		t.Error("It should be called once")
		t.Error(eventsCalled)
	}
	if countersCalled != 1 {
		t.Error("It should be called once")
		t.Error(countersCalled)
	}
	if gaugesCalled != 1 {
		t.Error("It should be called once")
		t.Error(gaugesCalled)
	}
	if latenciesCalled != 1 {
		t.Error("It should be called once")
		t.Error(latenciesCalled)
	}

	managerTest.Stop()
	time.Sleep(time.Second * 1)
	if splitFetchCalled != 2 {
		t.Error("It should be called twice")
		t.Error(splitFetchCalled)
	}
	if segmentFetchCalled != 4 {
		t.Error("It should be called fourth times")
		t.Error(segmentFetchCalled)
	}

	if impressionsCalled != 3 {
		t.Error("It should be called three times")
		t.Error(impressionsCalled)
	}
	if eventsCalled != 4 {
		t.Error("It should be called fourth times")
		t.Error(eventsCalled)
	}
	if countersCalled != 2 {
		t.Error("It should be called twice")
		t.Error(countersCalled)
	}
	if gaugesCalled != 2 {
		t.Error("It should be called twice")
		t.Error(gaugesCalled)
	}
	if latenciesCalled != 2 {
		t.Error("It should be called twice")
		t.Error(latenciesCalled)
	}

	msg := <-readyChannel
	switch msg {
	case "READY":
		// Broadcast ready status for SDK
		atomic.AddInt64(&shouldBeReady, 1)
	default:
		t.Error("Wrong msg received")
	}

	if shouldBeReady != 1 {
		t.Error("It should be ready eventually")
	}
}
