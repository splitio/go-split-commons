package sync

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
	syncForTest := NewSynchronizerImpl(
		conf.TaskPeriods{CounterSync: 10, EventsSync: 10, GaugeSync: 10, ImpressionSync: 10, LatencySync: 10, SegmentSync: 10, SplitSync: 10},
		conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100, SegmentQueueSize: 50, SegmentWorkers: 5},
		&splitAPI,
		splitMockStorage,
		storageMock.MockSegmentStorage{},
		storageMock.MockMetricStorage{},
		storageMock.MockImpressionStorage{},
		storageMock.MockEventStorage{},
		logger,
		nil,
	)
	err := syncForTest.SyncAll()
	if err == nil {
		t.Error("It should return error")
	}
	if splitFetchCalled != 1 {
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
			segmentNames := set.NewSet()
			segmentNames.Add("segment1", "segment2")
			return segmentNames
		},
	}
	segmentMockStorage := storageMock.MockSegmentStorage{
		ChangeNumberCall: func(segmentName string) (int64, error) {
			return -1, nil
		},
	}
	syncForTest := NewSynchronizerImpl(
		conf.TaskPeriods{CounterSync: 10, EventsSync: 10, GaugeSync: 10, ImpressionSync: 10, LatencySync: 10, SegmentSync: 10, SplitSync: 10},
		conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100, SegmentQueueSize: 50, SegmentWorkers: 5},
		&splitAPI,
		splitMockStorage,
		segmentMockStorage,
		storageMock.MockMetricStorage{},
		storageMock.MockImpressionStorage{},
		storageMock.MockEventStorage{},
		logger,
		nil,
	)
	err := syncForTest.SyncAll()
	if err == nil {
		t.Error("It should return error")
	}
	if splitFetchCalled != 1 {
		t.Error("It should be called once")
	}
	if segmentFetchCalled != 2 {
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
			segmentNames := set.NewSet()
			segmentNames.Add("segment1", "segment2")
			return segmentNames
		},
	}
	segmentMockStorage := storageMock.MockSegmentStorage{
		ChangeNumberCall: func(segmentName string) (int64, error) {
			return -1, nil
		},
		GetCall: func(segmentName string) *set.ThreadUnsafeSet {
			if segmentName != "segment1" && segmentName != "segment2" {
				t.Error("Wrong name")
			}
			return nil
		},
		PutCall: func(name string, segment *set.ThreadUnsafeSet, changeNumber int64) {
			if name != "segment1" && name != "segment2" {
				t.Error("Wrong name")
			}
		},
	}
	syncForTest := NewSynchronizerImpl(
		conf.TaskPeriods{CounterSync: 10, EventsSync: 10, GaugeSync: 10, ImpressionSync: 10, LatencySync: 10, SegmentSync: 10, SplitSync: 10},
		conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100, SegmentQueueSize: 50, SegmentWorkers: 5},
		&splitAPI,
		splitMockStorage,
		segmentMockStorage,
		storageMock.MockMetricStorage{},
		storageMock.MockImpressionStorage{},
		storageMock.MockEventStorage{},
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
			segmentNames := set.NewSet()
			segmentNames.Add("segment1", "segment2")
			return segmentNames
		},
	}
	segmentMockStorage := storageMock.MockSegmentStorage{
		ChangeNumberCall: func(segmentName string) (int64, error) {
			return -1, nil
		},
		GetCall: func(segmentName string) *set.ThreadUnsafeSet {
			if segmentName != "segment1" && segmentName != "segment2" {
				t.Error("Wrong name")
			}
			return nil
		},
		PutCall: func(name string, segment *set.ThreadUnsafeSet, changeNumber int64) {
			if name != "segment1" && name != "segment2" {
				t.Error("Wrong name")
			}
		},
	}
	syncForTest := NewSynchronizerImpl(
		conf.TaskPeriods{CounterSync: 100, EventsSync: 100, GaugeSync: 100, ImpressionSync: 100, LatencySync: 100, SegmentSync: 10, SplitSync: 2},
		conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100, SegmentQueueSize: 50, SegmentWorkers: 5},
		&splitAPI,
		splitMockStorage,
		segmentMockStorage,
		storageMock.MockMetricStorage{},
		storageMock.MockImpressionStorage{},
		storageMock.MockEventStorage{},
		logger,
		nil,
	)
	syncForTest.StartPeriodicFetching()
	time.Sleep(time.Second * 2)
	if splitFetchCalled != 2 {
		t.Error("It should be called twice")
		t.Error(splitFetchCalled)
	}
	if segmentFetchCalled != 2 {
		t.Error("It should be called twice")
		t.Error(segmentFetchCalled)
	}
	syncForTest.StopPeriodicFetching()
	time.Sleep(time.Second * 3)
	if splitFetchCalled != 2 {
		t.Error("It should be called twice")
		t.Error(splitFetchCalled)
	}
	if segmentFetchCalled != 2 {
		t.Error("It should be called twice")
		t.Error(segmentFetchCalled)
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
			RecordCall: func(events []dtos.EventDTO) error {
				atomic.AddInt64(&eventsCalled, 1)
				if len(events) != 1 {
					t.Error("Wrong length")
				}
				return nil
			},
		},
		ImpressionRecorder: httpMocks.MockImpressionRecorder{
			RecordCall: func(impressions []dtos.Impression) error {
				atomic.AddInt64(&impressionsCalled, 1)
				if len(impressions) != 1 {
					t.Error("Wrong length")
				}
				return nil
			},
		},
		MetricRecorder: httpMocks.MockMetricRecorder{
			RecordCountersCall: func(counters []dtos.CounterDTO) error {
				atomic.AddInt64(&countersCalled, 1)
				if len(counters) != 1 {
					t.Error("Wrong length")
				}
				return nil
			},
			RecordGaugeCall: func(gauge dtos.GaugeDTO) error {
				atomic.AddInt64(&gaugesCalled, 1)
				if gauge.MetricName != "gauge" {
					t.Error("Wrong gauge")
				}
				return nil
			},
			RecordLatenciesCall: func(latencies []dtos.LatenciesDTO) error {
				atomic.AddInt64(&latenciesCalled, 1)
				if len(latencies) != 1 {
					t.Error("Wrong length")
				}
				return nil
			},
		},
	}
	syncForTest := NewSynchronizerImpl(
		conf.TaskPeriods{CounterSync: 10, EventsSync: 10, GaugeSync: 10, ImpressionSync: 10, LatencySync: 10, SegmentSync: 100, SplitSync: 100},
		conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100, SegmentQueueSize: 50, SegmentWorkers: 5},
		&splitAPI,
		storageMock.MockSplitStorage{},
		storageMock.MockSegmentStorage{},
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
	)
	syncForTest.StartPeriodicDataRecording()
	time.Sleep(time.Second * 1)
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
	syncForTest.StopPeriodicDataRecording()
	time.Sleep(time.Second * 1)
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
}

/*
func TestSyncAll(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitAPI := service.SplitAPI{
		SplitFetcher: httpMocks.MockSplitFetcher{
			FetchCall: func(changeNumber int64) (*dtos.SplitChangesDTO, error) {
				if changeNumber != -1 {
					t.Error("Wrong changenumber passed")
				}
				return nil, errors.New("Some")
			},
		},
	}
	var s1Requested int64
	var s2Requested int64
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
			s1 := splits[0]
			if s1.Name != "split1" || s1.Killed {
				t.Error("split1 stored/retrieved incorrectly")
				t.Error(s1)
			}
			s2 := splits[1]
			if s2.Name != "split2" || !s2.Killed {
				t.Error("split2 stored/retrieved incorrectly")
				t.Error(s2)
			}
		},
		RemoveCall: func(splitname string) {
			if splitname != "split3" {
				t.Error("It should remove split3")
			}
		},
	}
	segmentMockStorage := storageMock.MockSegmentStorage{
		ChangeNumberCall: func(segmentName string) (int64, error) {
			switch segmentName {
			case "segment1":
				if s1Requested >= 1 {
					return 123, nil
				}
			case "segment2":
				if s2Requested >= 1 {
					return 123, nil
				}
			default:
				t.Error("Wrong case")
			}
			return -1, nil
		},
		GetCall: func(segmentName string) *set.ThreadUnsafeSet {
			if segmentName != "segment1" && segmentName != "segment2" {
				t.Error("Wrong name")
			}
			switch segmentName {
			case "segment1":
			case "segment2":
				return nil
			default:
				t.Error("Wrong case")
			}
			return nil
		},
		PutCall: func(name string, segment *set.ThreadUnsafeSet, changeNumber int64) {
			switch name {
			case "segment1":
				if !segment.Has("item1") {
					t.Error("Wrong key in segment")
				}
				atomic.AddInt64(&s1Requested, 1)
			case "segment2":
				if !segment.Has("item5") {
					t.Error("Wrong key in segment")
				}
				atomic.AddInt64(&s2Requested, 1)
			default:
				t.Error("Wrong case")
			}
		},
	}
	metricMockStorage := storageMock.MockMetricStorage{
		PopLatenciesCall: func() []dtos.LatenciesDTO {
			toReturn := make([]dtos.LatenciesDTO, 0, 1)
			toReturn = append(toReturn, dtos.LatenciesDTO{
				MetricName: "latency",
				Latencies:  []int64{1, 2, 3},
			})
			return toReturn
		},
	}
	var call int64
	impression1 := dtos.Impression{
		BucketingKey: "someBucketingKey1",
		ChangeNumber: 123456789,
		FeatureName:  "someFeature1",
		KeyName:      "someKey1",
		Label:        "someLabel",
		Time:         123456789,
		Treatment:    "someTreatment1",
	}
	impression2 := dtos.Impression{
		BucketingKey: "someBucketingKey2",
		ChangeNumber: 123456789,
		FeatureName:  "someFeature2",
		KeyName:      "someKey2",
		Label:        "someLabel",
		Time:         123456789,
		Treatment:    "someTreatment2",
	}
	impression3 := dtos.Impression{
		BucketingKey: "someBucketingKey3",
		ChangeNumber: 123456789,
		FeatureName:  "someFeature3",
		KeyName:      "someKey3",
		Label:        "someLabel",
		Time:         123456789,
		Treatment:    "someTreatment3",
	}
	impressionMockStorage := storageMock.MockImpressionStorage{
		PopNCall: func(n int64) ([]dtos.Impression, error) {
			atomic.AddInt64(&call, 1)
			if n != 50 {
				t.Error("Wrong input parameter passed")
			}
			return []dtos.Impression{impression1, impression2, impression3}, nil
		},
		EmptyCall: func() bool {
			if call == 1 {
				return false
			}
			return true
		},
	}
	var call2 int64
	mockedEvent1 := dtos.EventDTO{EventTypeID: "someId", Key: "someKey1", Properties: nil, Timestamp: 123456789, TrafficTypeName: "someTraffic", Value: nil}
	mockedEvent2 := dtos.EventDTO{EventTypeID: "someId", Key: "someKey2", Properties: nil, Timestamp: 123456789, TrafficTypeName: "someTraffic", Value: nil}
	mockedEvent3 := dtos.EventDTO{EventTypeID: "someId", Key: "someKey3", Properties: nil, Timestamp: 123456789, TrafficTypeName: "someTraffic", Value: nil}
	eventMockStorage := storageMock.MockEventStorage{
		PopNCall: func(n int64) ([]dtos.EventDTO, error) {
			atomic.AddInt64(&call2, 1)
			if n != 50 {
				t.Error("Wrong input parameter passed")
			}
			return []dtos.EventDTO{mockedEvent1, mockedEvent2, mockedEvent3}, nil
		},
		EmptyCall: func() bool {
			if call2 == 1 {
				return false
			}
			return true
		},
	}
	syncForTest := NewSynchronizerImpl(
		conf.TaskPeriods{CounterSync: 10, EventsSync: 10, GaugeSync: 10, ImpressionSync: 10, LatencySync: 10, SegmentSync: 10, SplitSync: 10},
		conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100, SegmentQueueSize: 50, SegmentWorkers: 5},
		&splitAPI,
		splitMockStorage,
		segmentMockStorage,
		metricMockStorage,
		impressionMockStorage,
		eventMockStorage,
		logger,
		nil,
	)
	syncForTest.SyncAll()
}
*/
