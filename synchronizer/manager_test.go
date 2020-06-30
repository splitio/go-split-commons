package synchronizer

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync"
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

	manager, err := NewSynchronizerManager(&SynchronizerImpl{}, logger, conf.AdvancedConfig{}, httpMocks.MockAuthClient{}, storageMock.MockSplitStorage{}, make(chan int))
	if err == nil {
		t.Error("It should return err")
	}
	if manager != nil {
		t.Error("It should be nil")
	}

	manager, err = NewSynchronizerManager(&SynchronizerImpl{}, logger, conf.AdvancedConfig{}, httpMocks.MockAuthClient{}, storageMock.MockSplitStorage{}, make(chan int))
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

	statusChan := make(chan int, 1)
	managerTest, err := NewSynchronizerManager(
		mockSync,
		logger,
		advanced,
		mocks.MockAuthClient{
			AuthenticateCall: func() (*dtos.Token, error) {
				return nil, errors.New("some")
			},
		},
		storageMock.MockSplitStorage{},
		statusChan,
	)
	if err != nil {
		t.Error("It should not return err")
	}
	go managerTest.Start()

	msg := <-statusChan
	if msg != Error {
		t.Error("It should be err")
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

	managerTest, err := NewSynchronizerManager(
		mockSync,
		logger,
		advanced,
		mocks.MockAuthClient{
			AuthenticateCall: func() (*dtos.Token, error) {
				return nil, nil
			},
		},
		storageMock.MockSplitStorage{},
		make(chan int, 1),
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

	managerTest, err := NewSynchronizerManager(
		mockSync,
		logger,
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
		make(chan int, 1),
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

	streamingStatus := make(chan int, 1)
	pushManager, err := push.NewPushManager(logger, nil, nil, storageMock.MockSplitStorage{}, &advanced, streamingStatus)
	if err != nil {
		t.Error("It should not return err")
	}

	managerTest := Manager{
		syncMock.MockSynchronizer{
			SyncAllCall: func() error {
				return nil
			},
			StartPeriodicDataRecordingCall: func() {
				atomic.AddInt64(&periodicDataRecording, 1)
			},
			StartPeriodicFetchingCall: func() {
				atomic.AddInt64(&periodicDataFetching, 1)
			},
		},
		logger,
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
		make(chan int, 1),
		streamingStatus,
		false,
		&sync.RWMutex{},
	}

	go managerTest.Start()

	time.Sleep(1 * time.Second)

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

	statusChannel := make(chan int, 1)
	managerTest, err := NewSynchronizerManager(
		syncForTest,
		logger,
		advanced,
		mocks.MockAuthClient{
			AuthenticateCall: func() (*dtos.Token, error) {
				return nil, nil
			},
		},
		storageMock.MockSplitStorage{},
		statusChannel,
	)
	if err != nil {
		t.Error("It should not return err")
	}
	go managerTest.Start()

	msg := <-statusChannel
	switch msg {
	case Ready:
		// Broadcast ready status for SDK
		atomic.AddInt64(&shouldBeReady, 1)
	default:
		t.Error("Wrong msg received")
	}

	time.Sleep(2 * time.Second)

	if atomic.LoadInt64(&splitFetchCalled) != 2 {
		t.Error("It should be called twice")
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
	if atomic.LoadInt64(&splitFetchCalled) != 2 {
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
	if shouldBeReady != 1 {
		t.Error("It should be ready eventually")
	}
}

func TestStreaming(t *testing.T) {
	var shouldBeReady int64
	shouldBeReadyStreaming := 0
	var impressionsCalled int64
	var eventsCalled int64
	var countersCalled int64
	var gaugesCalled int64
	var latenciesCalled int64
	var splitFetchCalled int64
	var segmentFetchCalled int64
	var kilLocallyCalled int64

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, err := w.(http.Flusher)
		if !err {
			t.Error("Unexpected error")
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")

		for i := 0; i <= 3; i++ {
			switch i {
			case 0:
				sseMock, _ := ioutil.ReadFile("../testdata/sse.json")
				var mockedData map[string]interface{}
				_ = json.Unmarshal(sseMock, &mockedData)
				mockedStr, _ := json.Marshal(mockedData)
				fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
			case 1:
				mockedData := make(map[string]interface{})
				mockedData["event"] = "keepalive"
				mockedStr, _ := json.Marshal(mockedData)
				fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
			case 2:
				sseMock, _ := ioutil.ReadFile("../testdata/sse2.json")
				var mockedData map[string]interface{}
				_ = json.Unmarshal(sseMock, &mockedData)
				mockedStr, _ := json.Marshal(mockedData)
				fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
			case 3:
				sseMock, _ := ioutil.ReadFile("../testdata/sse3.json")
				var mockedData map[string]interface{}
				_ = json.Unmarshal(sseMock, &mockedData)
				mockedStr, _ := json.Marshal(mockedData)
				fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
			}
			flusher.Flush()
		}
	}))
	defer ts.Close()

	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}
	mockedSplit2 := dtos.SplitDTO{Name: "split2", Killed: true, Status: "ACTIVE", TrafficTypeName: "two"}
	advanced := conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100,
		SegmentQueueSize: 50, SegmentWorkers: 5, StreamingEnabled: true, StreamingServiceURL: ts.URL, SegmentUpdateQueueSize: 5000, SplitUpdateQueueSize: 5000}
	logger := logging.NewLogger(&logging.LoggerOptions{})

	splitAPI := service.SplitAPI{
		SplitFetcher: httpMocks.MockSplitFetcher{
			FetchCall: func(changeNumber int64) (*dtos.SplitChangesDTO, error) {
				atomic.AddInt64(&splitFetchCalled, 1)
				switch atomic.LoadInt64(&splitFetchCalled) {
				case 1:
					if changeNumber != -1 {
						t.Error("Wrong changenumber passed")
					}

					return &dtos.SplitChangesDTO{
						Splits: []dtos.SplitDTO{mockedSplit1, mockedSplit2},
						Since:  123,
						Till:   123,
					}, nil
				case 2:
					if changeNumber != 123 {
						t.Error("Wrong changenumber passed")
					}

					return &dtos.SplitChangesDTO{
						Splits: []dtos.SplitDTO{mockedSplit1, mockedSplit2},
						Since:  1591996685190,
						Till:   1591996685190,
					}, nil
				case 3:
					if changeNumber != 1591996685190 {
						t.Error("Wrong changenumber passed")
					}

					return &dtos.SplitChangesDTO{
						Splits: []dtos.SplitDTO{mockedSplit1, mockedSplit2},
						Since:  1591996754396,
						Till:   1591996754396,
					}, nil

				default:
					t.Error("Unexpected FetchCall")
					return nil, nil
				}
			},
		},
		SegmentFetcher: httpMocks.MockSegmentFetcher{
			FetchCall: func(name string, changeNumber int64) (*dtos.SegmentChangesDTO, error) {
				atomic.AddInt64(&segmentFetchCalled, 1)

				switch atomic.LoadInt64(&shouldBeReady) {
				case 0:
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
				case 1:
					if name != "TEST_SEGMENT" {
						t.Error("Wrong name")
					}
					return &dtos.SegmentChangesDTO{
						Name:    name,
						Added:   []string{"test_key"},
						Removed: []string{},
						Since:   1591988398533,
						Till:    1591988398533,
					}, nil
				default:
					t.Error("Unexpected FetchCall")
					return nil, nil
				}
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

	splitStorageMock := storageMock.MockSplitStorage{
		KillLocallyCall: func(splitName, defaultTreatment string) {
			atomic.AddInt64(&kilLocallyCalled, 1)
			if splitName != "test" {
				t.Error("Wrong split name passed")
			}
			if defaultTreatment != "some" {
				t.Error("Wrong defaultTreatment passed")
			}
		},
		ChangeNumberCall: func() (int64, error) {
			switch atomic.LoadInt64(&splitFetchCalled) {
			case 0:
				return -1, nil
			case 1:
				return 123, nil
			case 2:
				return 1591996685190, nil
			case 3:
				return 1591996754396, nil
			default:
				t.Error("Unexpected ChangeNumber call")
				return -1, nil
			}
		},
		PutManyCall: func(splits []dtos.SplitDTO, changeNumber int64) {
			switch atomic.LoadInt64(&splitFetchCalled) {
			case 1:
				if changeNumber != 123 {
					t.Error("Wrong changenumber")
				}
				if len(splits) != 2 {
					t.Error("Wrong length of passed splits")
				}
			case 2:
				if changeNumber != 1591996685190 {
					t.Error("Wrong changenumber")
				}
			case 3:
				if changeNumber != 1591996754396 {
					t.Error("Wrong changenumber")
				}
			default:
				t.Error("Unexpected PutMany call")
			}
		},
		SegmentNamesCall: func() *set.ThreadUnsafeSet {
			segmentNames := set.NewSet("segment1", "segment2")
			return segmentNames
		},
	}

	syncForTest := NewSynchronizer(
		conf.TaskPeriods{CounterSync: 10, EventsSync: 10, GaugeSync: 10, ImpressionSync: 10, LatencySync: 10, SegmentSync: 10, SplitSync: 10},
		advanced,
		&splitAPI,
		splitStorageMock,
		storageMock.MockSegmentStorage{
			ChangeNumberCall: func(segmentName string) (int64, error) {
				return -1, nil
			},
			KeysCall: func(segmentName string) *set.ThreadUnsafeSet {
				if segmentName != "segment1" && segmentName != "segment2" && segmentName != "TEST_SEGMENT" {
					t.Error("Wrong name")
				}
				return nil
			},
			UpdateCall: func(name string, toAdd *set.ThreadUnsafeSet, toRemove *set.ThreadUnsafeSet, changeNumber int64) error {
				switch atomic.LoadInt64(&shouldBeReady) {
				case 0:
					if name != "segment1" && name != "segment2" {
						t.Error("Wrong name")
					}
				case 1:
					if name != "TEST_SEGMENT" {
						t.Error("Wrong name")
					}
				default:
					t.Error("Unexpected UpdateCall")
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

	statusChannel := make(chan int, 1)
	managerTest, err := NewSynchronizerManager(
		syncForTest,
		logger,
		advanced,
		mocks.MockAuthClient{
			AuthenticateCall: func() (*dtos.Token, error) {
				return &dtos.Token{
					Token:       "eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE1OTE3NDQzOTksImlhdCI6MTU5MTc0MDc5OX0.EcWYtI0rlA7LCVJ5tYldX-vpfMRIc_1HT68-jhXseCo",
					PushEnabled: true,
				}, nil
			},
		},
		splitStorageMock,
		statusChannel,
	)
	if err != nil {
		t.Error("It should not return err")
	}

	go managerTest.Start()

	msg := <-statusChannel
	switch msg {
	case Ready:
		atomic.AddInt64(&shouldBeReady, 1)
	default:
		t.Error("Wrong msg received")
	}
	msg = <-statusChannel
	switch msg {
	case StreamingReady:
		shouldBeReadyStreaming++
	default:
		t.Error("Wrong msg received")
	}

	time.Sleep(1 * time.Second)
	managerTest.Stop()
	time.Sleep(1 * time.Second)

	if atomic.LoadInt64(&splitFetchCalled) != 3 {
		t.Error("It should be called three times")
	}
	if atomic.LoadInt64(&segmentFetchCalled) != 3 {
		t.Error("It should be called three times")
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
	if atomic.LoadInt64(&shouldBeReady) != 1 {
		t.Error("It should be ready eventually")
	}
	if shouldBeReadyStreaming != 1 {
		t.Error("It should be ready eventually")
	}
	if atomic.LoadInt64(&kilLocallyCalled) != 1 {
		t.Error("It should be called once")
	}
}

func TestStreamingAndSwitchToPolling(t *testing.T) {
	var splitFetchCalled int64
	var segmentFetchCalled int64

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, err := w.(http.Flusher)
		if !err {
			t.Error("Unexpected error")
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")

		for i := 0; i <= 3; i++ {
			time.Sleep(time.Duration(i*100) * time.Millisecond)
			switch i {
			case 0:
				sseMock, _ := ioutil.ReadFile("../testdata/occupancy.json")
				var mockedData map[string]interface{}
				_ = json.Unmarshal(sseMock, &mockedData)
				mockedStr, _ := json.Marshal(mockedData)
				fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
			case 1:
				sseMock, _ := ioutil.ReadFile("../testdata/occupancy2.json")
				var mockedData map[string]interface{}
				_ = json.Unmarshal(sseMock, &mockedData)
				mockedStr, _ := json.Marshal(mockedData)
				fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
			}
			flusher.Flush()
		}
	}))
	defer ts.Close()

	advanced := conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100,
		SegmentQueueSize: 50, SegmentWorkers: 5, StreamingEnabled: true, StreamingServiceURL: ts.URL, SegmentUpdateQueueSize: 5000, SplitUpdateQueueSize: 5000}
	logger := logging.NewLogger(&logging.LoggerOptions{})

	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}
	splitAPI := service.SplitAPI{
		SplitFetcher: httpMocks.MockSplitFetcher{
			FetchCall: func(changeNumber int64) (*dtos.SplitChangesDTO, error) {
				atomic.AddInt64(&splitFetchCalled, 1)
				return &dtos.SplitChangesDTO{
					Splits: []dtos.SplitDTO{mockedSplit1},
					Since:  1,
					Till:   1,
				}, nil
			},
		},
		SegmentFetcher: httpMocks.MockSegmentFetcher{
			FetchCall: func(name string, changeNumber int64) (*dtos.SegmentChangesDTO, error) {
				atomic.AddInt64(&segmentFetchCalled, 1)
				return &dtos.SegmentChangesDTO{
					Name:    name,
					Added:   []string{"some"},
					Removed: []string{},
					Since:   1,
					Till:    1,
				}, nil
			},
		},
		EventRecorder: httpMocks.MockEventRecorder{
			RecordCall: func(events []dtos.EventDTO, metadata dtos.Metadata) error {
				return nil
			},
		},
		ImpressionRecorder: httpMocks.MockImpressionRecorder{
			RecordCall: func(impressions []dtos.Impression, metadata dtos.Metadata) error {
				return nil
			},
		},
		MetricRecorder: httpMocks.MockMetricRecorder{
			RecordCountersCall: func(counters []dtos.CounterDTO, metadata dtos.Metadata) error {
				return nil
			},
			RecordGaugeCall: func(gauge dtos.GaugeDTO, metadata dtos.Metadata) error {
				return nil
			},
			RecordLatenciesCall: func(latencies []dtos.LatenciesDTO, metadata dtos.Metadata) error {
				return nil
			},
		},
	}

	splitStorageMock := storageMock.MockSplitStorage{
		KillLocallyCall: func(splitName, defaultTreatment string) {
		},
		ChangeNumberCall: func() (int64, error) {
			return -1, nil
		},
		PutManyCall: func(splits []dtos.SplitDTO, changeNumber int64) {
		},
		SegmentNamesCall: func() *set.ThreadUnsafeSet {
			return set.NewSet("one")
		},
	}

	syncForTest := NewSynchronizer(
		conf.TaskPeriods{CounterSync: 10, EventsSync: 10, GaugeSync: 10, ImpressionSync: 10, LatencySync: 10, SegmentSync: 10, SplitSync: 10},
		advanced,
		&splitAPI,
		splitStorageMock,
		storageMock.MockSegmentStorage{
			ChangeNumberCall: func(segmentName string) (int64, error) {
				return -1, nil
			},
			KeysCall: func(segmentName string) *set.ThreadUnsafeSet {
				return nil
			},
			UpdateCall: func(name string, toAdd *set.ThreadUnsafeSet, toRemove *set.ThreadUnsafeSet, changeNumber int64) error {
				return nil
			},
		},
		storageMock.MockMetricStorage{
			PopCountersCall: func() []dtos.CounterDTO {
				return []dtos.CounterDTO{}
			},
			PopGaugesCall: func() []dtos.GaugeDTO {
				return []dtos.GaugeDTO{}
			},
			PopLatenciesCall: func() []dtos.LatenciesDTO {
				return []dtos.LatenciesDTO{}
			},
			IncLatencyCall: func(metricName string, index int) {},
			IncCounterCall: func(key string) {},
			PutGaugeCall:   func(key string, gauge float64) {},
		},
		storageMock.MockImpressionStorage{
			PopNCall: func(n int64) ([]dtos.Impression, error) {
				return []dtos.Impression{}, nil
			},
			EmptyCall: func() bool {
				return true
			},
		},
		storageMock.MockEventStorage{
			PopNCall: func(n int64) ([]dtos.EventDTO, error) {
				return []dtos.EventDTO{}, nil
			},
			EmptyCall: func() bool {
				return true
			},
		},
		logger,
		nil,
		&dtos.Metadata{},
	)

	statusChannel := make(chan int, 1)
	managerTest, err := NewSynchronizerManager(
		syncForTest,
		logger,
		advanced,
		mocks.MockAuthClient{
			AuthenticateCall: func() (*dtos.Token, error) {
				return &dtos.Token{
					Token:       "eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE1OTE3NDQzOTksImlhdCI6MTU5MTc0MDc5OX0.EcWYtI0rlA7LCVJ5tYldX-vpfMRIc_1HT68-jhXseCo",
					PushEnabled: true,
				}, nil
			},
		},
		splitStorageMock,
		statusChannel,
	)
	if err != nil {
		t.Error("It should not return err")
	}

	go managerTest.Start()
	msg := <-statusChannel
	if msg != Ready {
		t.Error("It should send ready")
	}
	msg = <-statusChannel
	if msg != StreamingReady {
		t.Error("It should send streaming ready")
	}

	time.Sleep(2 * time.Second)
	managerTest.Stop()
	time.Sleep(1 * time.Second)

	if atomic.LoadInt64(&splitFetchCalled) != 2 {
		t.Error("It should be called once")
	}
	if atomic.LoadInt64(&segmentFetchCalled) != 2 {
		t.Error("It should be called once")
	}
}
