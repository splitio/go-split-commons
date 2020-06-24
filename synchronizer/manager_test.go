package synchronizer

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/push"
	"github.com/splitio/go-split-commons/service"
	"github.com/splitio/go-split-commons/service/mocks"
	httpMocks "github.com/splitio/go-split-commons/service/mocks"
	storageMock "github.com/splitio/go-split-commons/storage/mocks"
	syncMock "github.com/splitio/go-split-commons/synchronizer/mocks"
	"github.com/splitio/go-toolkit/datastructures/set"
	"github.com/splitio/go-toolkit/logging"
)

func TestSyncError(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	manager, err := NewSynchronizerManager(&SynchronizerImpl{}, logger, nil, conf.AdvancedConfig{}, httpMocks.MockAuthClient{}, storageMock.MockSplitStorage{})
	if err == nil {
		t.Error("It should return err")
	}
	if manager != nil {
		t.Error("It should be nil")
	}

	manager, err = NewSynchronizerManager(&SynchronizerImpl{}, logger, make(chan string), conf.AdvancedConfig{}, httpMocks.MockAuthClient{}, storageMock.MockSplitStorage{})
	if err == nil {
		t.Error("It should return err")
	}
	if manager != nil {
		t.Error("It should be nil")
	}
}

func TestSyncInvalidAuth(t *testing.T) {
	advanced := conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100,
		SegmentQueueSize: 50, SegmentWorkers: 5, StreamingEnabled: false, SegmentUpdateQueueSize: 5000, SplitUpdateQueueSize: 5000}
	logger := logging.NewLogger(&logging.LoggerOptions{})
	mockSync := syncMock.MockSynchronizer{
		SyncAllCall: func() error {
			t.Error("It should not be called")
			return nil
		},
		StartPeriodicDataRecordingCall: func() {
			t.Error("It should not be called")
		},
		StartPeriodicFetchingCall: func() {
			t.Error("It should not be called")
		},
	}

	readyChannel := make(chan string, 1)
	managerTest, err := NewSynchronizerManager(
		mockSync,
		logger,
		readyChannel,
		advanced,
		mocks.MockAuthClient{
			AuthenticateCall: func() (*dtos.Token, error) {
				return nil, errors.New("some")
			},
		},
		storageMock.MockSplitStorage{},
	)
	if err != nil {
		t.Error("It should not return err")
	}
	err = managerTest.Start()
	if err == nil {
		t.Error("It should return err")
	}
}

func TestPollingWithStreamingFalse(t *testing.T) {
	var periodicDataRecording int64
	var periodicDataFetching int64
	advanced := conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100,
		SegmentQueueSize: 50, SegmentWorkers: 5, StreamingEnabled: false, SegmentUpdateQueueSize: 5000, SplitUpdateQueueSize: 5000}
	logger := logging.NewLogger(&logging.LoggerOptions{})
	mockSync := syncMock.MockSynchronizer{
		SyncAllCall: func() error {
			return nil
		},
		StartPeriodicDataRecordingCall: func() {
			atomic.AddInt64(&periodicDataRecording, 1)
		},
		StartPeriodicFetchingCall: func() {
			atomic.AddInt64(&periodicDataFetching, 1)
		},
	}

	readyChannel := make(chan string, 1)
	managerTest, err := NewSynchronizerManager(
		mockSync,
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
	if err != nil {
		t.Error("It should not return err")
	}
	managerTest.Start()
	if atomic.LoadInt64(&periodicDataRecording) != 1 {
		t.Error("It should be called once")
	}
	if atomic.LoadInt64(&periodicDataFetching) != 1 {
		t.Error("It should be called once")
	}
}

func TestPollingWithStreamingPushFalse(t *testing.T) {
	var periodicDataRecording int64
	var periodicDataFetching int64
	advanced := conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100,
		SegmentQueueSize: 50, SegmentWorkers: 5, StreamingEnabled: false, SegmentUpdateQueueSize: 5000, SplitUpdateQueueSize: 5000}
	logger := logging.NewLogger(&logging.LoggerOptions{})
	mockSync := syncMock.MockSynchronizer{
		SyncAllCall: func() error {
			return nil
		},
		StartPeriodicDataRecordingCall: func() {
			atomic.AddInt64(&periodicDataRecording, 1)
		},
		StartPeriodicFetchingCall: func() {
			atomic.AddInt64(&periodicDataFetching, 1)
		},
	}

	readyChannel := make(chan string, 1)
	managerTest, err := NewSynchronizerManager(
		mockSync,
		logger,
		readyChannel,
		advanced,
		mocks.MockAuthClient{
			AuthenticateCall: func() (*dtos.Token, error) {
				return &dtos.Token{
					Token:       "some",
					PushEnabled: false,
				}, nil
			},
		},
		storageMock.MockSplitStorage{},
	)
	if err != nil {
		t.Error("It should not return err")
	}
	managerTest.Start()
	if atomic.LoadInt64(&periodicDataRecording) != 1 {
		t.Error("It should be called once")
	}
	if atomic.LoadInt64(&periodicDataFetching) != 1 {
		t.Error("It should be called once")
	}
}

func TestPollingWithStreamingPushError(t *testing.T) {
	var periodicDataRecording int64
	var periodicDataFetching int64
	advanced := conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100,
		SegmentQueueSize: 50, SegmentWorkers: 5, StreamingEnabled: true, StreamingServiceURL: "some", SegmentUpdateQueueSize: 5000, SplitUpdateQueueSize: 5000}
	logger := logging.NewLogger(&logging.LoggerOptions{})
	mockSync := syncMock.MockSynchronizer{
		SyncAllCall: func() error {
			return nil
		},
		StartPeriodicDataRecordingCall: func() {
			atomic.AddInt64(&periodicDataRecording, 1)
		},
		StartPeriodicFetchingCall: func() {
			atomic.AddInt64(&periodicDataFetching, 1)
		},
	}

	readyChannel := make(chan string, 1)
	pushManager, err := push.NewPushManager(logger, nil, nil, storageMock.MockSplitStorage{}, &advanced)
	if err != nil {
		t.Error("It should not return err")
	}
	managerTest := Manager{
		mockSync,
		logger,
		readyChannel,
		advanced,
		mocks.MockAuthClient{
			AuthenticateCall: func() (*dtos.Token, error) {
				return &dtos.Token{
					Token:       "eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE1OTE3NDQzOTksImlhdCI6MTU5MTc0MDc5OX0.EcWYtI0rlA7LCVJ5tYldX-vpfMRIc_1HT68-jhXseCo",
					PushEnabled: true,
				}, nil
			},
		},
		pushManager,
	}
	managerTest.Start()
	if atomic.LoadInt64(&periodicDataRecording) != 1 {
		t.Error("It should be called once")
	}
	if atomic.LoadInt64(&periodicDataFetching) != 1 {
		t.Error("It should be called once")
	}
}

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
	advanced := conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100,
		SegmentQueueSize: 50, SegmentWorkers: 5, StreamingEnabled: false, StreamingServiceURL: "some", SegmentUpdateQueueSize: 5000, SplitUpdateQueueSize: 5000}
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
	managerTest, err := NewSynchronizerManager(
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
	if err != nil {
		t.Error("It should not return err")
	}
	managerTest.Start()

	time.Sleep(time.Second * 1)
	if atomic.LoadInt64(&splitFetchCalled) != 2 {
		t.Error("It should be called twice")
		t.Error(splitFetchCalled)
	}
	if atomic.LoadInt64(&segmentFetchCalled) != 4 {
		t.Error("It should be called fourth times")
	}
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

	managerTest.Stop()
	time.Sleep(time.Second * 1)
	if splitFetchCalled != 2 {
		t.Error("It should be called twice")
		t.Error(splitFetchCalled)
	}
	if atomic.LoadInt64(&segmentFetchCalled) != 4 {
		t.Error("It should be called fourth times")
	}

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
