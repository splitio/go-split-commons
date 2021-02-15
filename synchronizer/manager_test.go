package synchronizer

/*
import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v2/conf"
	"github.com/splitio/go-split-commons/v2/dtos"
	"github.com/splitio/go-split-commons/v2/push"
	pushMock "github.com/splitio/go-split-commons/v2/push/mocks"
	"github.com/splitio/go-split-commons/v2/service"
	"github.com/splitio/go-split-commons/v2/service/mocks"
	httpMocks "github.com/splitio/go-split-commons/v2/service/mocks"
	"github.com/splitio/go-split-commons/v2/storage"
	storageMock "github.com/splitio/go-split-commons/v2/storage/mocks"
	syncMock "github.com/splitio/go-split-commons/v2/synchronizer/mocks"
	"github.com/splitio/go-split-commons/v2/synchronizer/worker/event"
	"github.com/splitio/go-split-commons/v2/synchronizer/worker/impression"
	"github.com/splitio/go-split-commons/v2/synchronizer/worker/metric"
	"github.com/splitio/go-split-commons/v2/synchronizer/worker/segment"
	"github.com/splitio/go-split-commons/v2/synchronizer/worker/split"
	"github.com/splitio/go-split-commons/v2/tasks"
	"github.com/splitio/go-toolkit/v4/datastructures/set"
	"github.com/splitio/go-toolkit/v4/logging"
)

func TestSyncError(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	manager, err := NewSynchronizerManager(&SynchronizerImpl{}, logger, conf.AdvancedConfig{}, httpMocks.MockAuthClient{}, storageMock.MockSplitStorage{}, make(chan int64))
	if err == nil {
		t.Error("It should return err")
	}
	if manager != nil {
		t.Error("It should be nil")
	}

	manager, err = NewSynchronizerManager(&SynchronizerImpl{}, logger, conf.AdvancedConfig{}, httpMocks.MockAuthClient{}, storageMock.MockSplitStorage{}, make(chan int64))
	if err == nil {
		t.Error("It should return err")
	}
	if manager != nil {
		t.Error("It should be nil")
	}
}

func TestPollingWithStreamingFalse(t *testing.T) {
	var periodicDataRecording int64
	var periodicDataFetching int64
	advanced := conf.GetDefaultAdvancedConfig()
	advanced.StreamingEnabled = false
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

	status := make(chan int64, 1)
	stat := atomic.Value{}
	stat.Store(Idle)
	managerTest := ManagerImpl{
		synchronizer:    mockSync,
		logger:          logger,
		config:          advanced,
		managerStatus:   make(chan int64, 1),
		pushManager:     pushMock.MockManager{},
		streamingStatus: status,
		status:          stat,
	}
	managerTest.Start()
	if managerTest.status.Load().(int) != Polling {
		t.Error("It should started in Polling mode")
	}
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
	advanced := conf.GetDefaultAdvancedConfig()
	advanced.StreamingEnabled = false
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
		make(chan int64, 1),
	)
	if err != nil {
		t.Error("It should not return err")
	}
	managerTest.Start()
	if managerTest.status.Load().(int) != Polling {
		t.Error("It should started in Polling mode")
	}
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
	advanced := conf.GetDefaultAdvancedConfig()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusInternalServerError)
	}))
	defer ts.Close()

	advanced.StreamingServiceURL = ts.URL
	logger := logging.NewLogger(&logging.LoggerOptions{})

	streamingStatus := make(chan int64, 1)
	pushManager, _ := push.NewManager(logger, nil, &advanced, streamingStatus, mocks.MockAuthClient{
		AuthenticateCall: func() (*dtos.Token, error) {
			return &dtos.Token{
				Token:       "eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE1OTE3NDQzOTksImlhdCI6MTU5MTc0MDc5OX0.EcWYtI0rlA7LCVJ5tYldX-vpfMRIc_1HT68-jhXseCo",
				PushEnabled: true,
			}, nil
		},
	})

	status := atomic.Value{}
	status.Store(Idle)
	managerStatus := make(chan int64, 1)
	managerTest := ManagerImpl{
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
		pushManager,
		managerStatus,
		streamingStatus,
		status,
	}

	go managerTest.Start()
	<-managerStatus
	time.Sleep(250 * time.Millisecond)
	if managerTest.status.Load().(int) != Polling {
		t.Error("It should started in Polling mode")
	}
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
		SegmentQueueSize: 50, SegmentWorkers: 5, StreamingEnabled: false, SegmentUpdateQueueSize: 5000, SplitUpdateQueueSize: 5000}
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
	eventStorageMock := storageMock.MockEventStorage{
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
	mockSplitStorage := storageMock.MockSplitStorage{
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
	metricStorageMock := storageMock.MockMetricStorage{
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
	}
	segmentStorageMock := storageMock.MockSegmentStorage{
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
	impressionStorageMock := storageMock.MockImpressionStorage{
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
	metricTestWrapper := storage.NewMetricWrapper(metricStorageMock, nil, logger)
	workers := Workers{
		SplitFetcher:       split.NewSplitFetcher(mockSplitStorage, splitAPI.SplitFetcher, metricTestWrapper, logger),
		SegmentFetcher:     segment.NewSegmentFetcher(mockSplitStorage, segmentStorageMock, splitAPI.SegmentFetcher, metricTestWrapper, logger),
		EventRecorder:      event.NewEventRecorderSingle(eventStorageMock, splitAPI.EventRecorder, metricTestWrapper, logger, dtos.Metadata{}),
		ImpressionRecorder: impression.NewRecorderSingle(impressionStorageMock, splitAPI.ImpressionRecorder, metricTestWrapper, logger, dtos.Metadata{}, conf.ManagerConfig{ImpressionsMode: conf.ImpressionsModeDebug}),
		TelemetryRecorder:  metric.NewRecorderSingle(metricStorageMock, splitAPI.MetricRecorder, dtos.Metadata{}),
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

	statusChannel := make(chan int64, 1)
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
	if managerTest.status.Load().(int) != Polling {
		t.Error("It should started in Polling mode")
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
	var impressionsCalled int64
	var eventsCalled int64
	var countersCalled int64
	var gaugesCalled int64
	var latenciesCalled int64
	var splitFetchCalled int64
	var segmentFetchCalled int64
	var kilLocallyCalled int64
	var putManyCalled int64

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, err := w.(http.Flusher)
		if !err {
			t.Error("Unexpected error")
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")

		sseMock, _ := ioutil.ReadFile("../testdata/sse.json")
		var mockedData map[string]interface{}
		_ = json.Unmarshal(sseMock, &mockedData)
		mockedStr, _ := json.Marshal(mockedData)
		fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
		flusher.Flush()

		time.Sleep(10 * time.Millisecond)
		mockedData = make(map[string]interface{})
		mockedData["event"] = "keepalive"
		mockedStr, _ = json.Marshal(mockedData)
		fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
		flusher.Flush()

		time.Sleep(10 * time.Millisecond)
		sseMock, _ = ioutil.ReadFile("../testdata/sse2.json")
		mockedData = make(map[string]interface{})
		_ = json.Unmarshal(sseMock, &mockedData)
		mockedStr, _ = json.Marshal(mockedData)
		fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
		flusher.Flush()

		time.Sleep(10 * time.Millisecond)
		sseMock, _ = ioutil.ReadFile("../testdata/sse3.json")
		mockedData = make(map[string]interface{})
		_ = json.Unmarshal(sseMock, &mockedData)
		mockedStr, _ = json.Marshal(mockedData)
		fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
		flusher.Flush()
		time.Sleep(5 * time.Second)
	}))
	defer ts.Close()

	advanced := conf.AdvancedConfig{EventsQueueSize: 100, EventsBulkSize: 100, HTTPTimeout: 100, ImpressionsBulkSize: 100, ImpressionsQueueSize: 100,
		SegmentQueueSize: 50, SegmentWorkers: 5, StreamingEnabled: true, StreamingServiceURL: ts.URL, SegmentUpdateQueueSize: 5000, SplitUpdateQueueSize: 5000}
	logger := logging.NewLogger(&logging.LoggerOptions{})

	splitAPI := service.SplitAPI{
		SplitFetcher: httpMocks.MockSplitFetcher{
			FetchCall: func(changeNumber int64) (*dtos.SplitChangesDTO, error) {
				atomic.AddInt64(&splitFetchCalled, 1)
				return &dtos.SplitChangesDTO{
					Splits: []dtos.SplitDTO{},
					Since:  1591996754396,
					Till:   1591996754396,
				}, nil
			},
		},
		SegmentFetcher: httpMocks.MockSegmentFetcher{
			FetchCall: func(name string, changeNumber int64) (*dtos.SegmentChangesDTO, error) {
				atomic.AddInt64(&segmentFetchCalled, 1)
				return &dtos.SegmentChangesDTO{
					Name:    name,
					Added:   []string{"test_key"},
					Removed: []string{},
					Since:   1591988398533,
					Till:    1591988398533,
				}, nil
			},
		},
		EventRecorder: httpMocks.MockEventRecorder{
			RecordCall: func(events []dtos.EventDTO, metadata dtos.Metadata) error {
				atomic.AddInt64(&eventsCalled, 1)
				return nil
			},
		},
		ImpressionRecorder: httpMocks.MockImpressionRecorder{
			RecordCall: func(impressions []dtos.ImpressionsDTO, metadata dtos.Metadata, extraHeaders map[string]string) error {
				atomic.AddInt64(&impressionsCalled, 1)
				return nil
			},
		},
		MetricRecorder: httpMocks.MockMetricRecorder{
			RecordCountersCall: func(counters []dtos.CounterDTO, metadata dtos.Metadata) error {
				atomic.AddInt64(&countersCalled, 1)
				return nil
			},
			RecordGaugeCall: func(gauge dtos.GaugeDTO, metadata dtos.Metadata) error {
				atomic.AddInt64(&gaugesCalled, 1)
				return nil
			},
			RecordLatenciesCall: func(latencies []dtos.LatenciesDTO, metadata dtos.Metadata) error {
				atomic.AddInt64(&latenciesCalled, 1)
				return nil
			},
		},
	}
	segmentStorageMock := storageMock.MockSegmentStorage{
		ChangeNumberCall: func(segmentName string) (int64, error) {
			return -1, nil
		},
		KeysCall: func(segmentName string) *set.ThreadUnsafeSet {
			return nil
		},
		UpdateCall: func(name string, toAdd *set.ThreadUnsafeSet, toRemove *set.ThreadUnsafeSet, changeNumber int64) error {
			return nil
		},
	}
	splitStorageMock := storageMock.MockSplitStorage{
		KillLocallyCall: func(splitName, defaultTreatment string, changeNumber int64) {
			atomic.AddInt64(&kilLocallyCalled, 1)
			if splitName != "test" {
				t.Error("Wrong split name passed")
			}
			if defaultTreatment != "some" {
				t.Error("Wrong defaultTreatment passed")
			}
			if changeNumber != 1591996754396 {
				t.Error("Wrong changeNumber passed")
			}
		},
		ChangeNumberCall: func() (int64, error) {
			return 123, nil
		},
		PutManyCall: func(splits []dtos.SplitDTO, changeNumber int64) {
			atomic.AddInt64(&putManyCalled, 1)
		},
		SegmentNamesCall: func() *set.ThreadUnsafeSet {
			return set.NewSet("segment1")
		},
	}
	metricStorageMock := storageMock.MockMetricStorage{
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
	}
	impressionStorageMock := storageMock.MockImpressionStorage{
		PopNCall: func(n int64) ([]dtos.Impression, error) {
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
			return true
		},
	}
	eventStorageMock := storageMock.MockEventStorage{
		PopNCall: func(n int64) ([]dtos.EventDTO, error) {
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
			return true
		},
	}
	metricTestWrapper := storage.NewMetricWrapper(metricStorageMock, nil, logger)
	workers := Workers{
		SplitFetcher:       split.NewSplitFetcher(splitStorageMock, splitAPI.SplitFetcher, metricTestWrapper, logger),
		SegmentFetcher:     segment.NewSegmentFetcher(splitStorageMock, segmentStorageMock, splitAPI.SegmentFetcher, metricTestWrapper, logger),
		EventRecorder:      event.NewEventRecorderSingle(eventStorageMock, splitAPI.EventRecorder, metricTestWrapper, logger, dtos.Metadata{}),
		ImpressionRecorder: impression.NewRecorderSingle(impressionStorageMock, splitAPI.ImpressionRecorder, metricTestWrapper, logger, dtos.Metadata{}, conf.ManagerConfig{ImpressionsMode: conf.ImpressionsModeDebug}),
		TelemetryRecorder:  metric.NewRecorderSingle(metricStorageMock, splitAPI.MetricRecorder, dtos.Metadata{}),
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

	statusChannel := make(chan int64, 1)
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

	time.Sleep(100 * time.Millisecond)
	if managerTest.status.Load().(int) != Streaming {
		t.Error("It should started in Streaming mode")
	}

	time.Sleep(1 * time.Second)
	managerTest.Stop()
	time.Sleep(1 * time.Second)

	if atomic.LoadInt64(&splitFetchCalled) != 4 {
		t.Error("It should be called fourth times")
	}
	if atomic.LoadInt64(&segmentFetchCalled) != 3 {
		t.Error("It should be called three times")
	}
	if atomic.LoadInt64(&impressionsCalled) != 1 {
		t.Error("It should be called once")
	}
	if atomic.LoadInt64(&eventsCalled) != 1 {
		t.Error("It should be called once")
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
	if atomic.LoadInt64(&kilLocallyCalled) != 1 {
		t.Error("It should be called once")
	}
	if atomic.LoadInt64(&putManyCalled) != 4 {
		t.Error("It should be called fourth times")
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

		time.Sleep(time.Duration(10) * time.Millisecond)
		sseMock, _ := ioutil.ReadFile("../testdata/occupancy.json")
		var mockedData map[string]interface{}
		_ = json.Unmarshal(sseMock, &mockedData)
		mockedStr, _ := json.Marshal(mockedData)
		fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
		flusher.Flush()

		time.Sleep(time.Duration(10) * time.Millisecond)
		sseMock, _ = ioutil.ReadFile("../testdata/occupancy2.json")
		mockedData = make(map[string]interface{})
		_ = json.Unmarshal(sseMock, &mockedData)
		mockedStr, _ = json.Marshal(mockedData)
		fmt.Fprintf(w, "data: %s\n\n", string(mockedStr))
		flusher.Flush()
		time.Sleep(5 * time.Second)
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
			RecordCall: func(impressions []dtos.ImpressionsDTO, metadata dtos.Metadata, extraHeaders map[string]string) error {
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
		KillLocallyCall: func(splitName, defaultTreatment string, changeNumber int64) {
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
	segmentStorageMock := storageMock.MockSegmentStorage{
		ChangeNumberCall: func(segmentName string) (int64, error) {
			return -1, nil
		},
		KeysCall: func(segmentName string) *set.ThreadUnsafeSet {
			return nil
		},
		UpdateCall: func(name string, toAdd *set.ThreadUnsafeSet, toRemove *set.ThreadUnsafeSet, changeNumber int64) error {
			return nil
		},
	}
	metricStorageMock := storageMock.MockMetricStorage{
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
	}
	impressionStorageMock := storageMock.MockImpressionStorage{
		PopNCall: func(n int64) ([]dtos.Impression, error) {
			return []dtos.Impression{}, nil
		},
		EmptyCall: func() bool {
			return true
		},
	}
	eventStorageMock := storageMock.MockEventStorage{
		PopNCall: func(n int64) ([]dtos.EventDTO, error) {
			return []dtos.EventDTO{}, nil
		},
		EmptyCall: func() bool {
			return true
		},
	}
	metricTestWrapper := storage.NewMetricWrapper(metricStorageMock, nil, logger)
	workers := Workers{
		SplitFetcher:       split.NewSplitFetcher(splitStorageMock, splitAPI.SplitFetcher, metricTestWrapper, logger),
		SegmentFetcher:     segment.NewSegmentFetcher(splitStorageMock, segmentStorageMock, splitAPI.SegmentFetcher, metricTestWrapper, logger),
		EventRecorder:      event.NewEventRecorderSingle(eventStorageMock, splitAPI.EventRecorder, metricTestWrapper, logger, dtos.Metadata{}),
		ImpressionRecorder: impression.NewRecorderSingle(impressionStorageMock, splitAPI.ImpressionRecorder, metricTestWrapper, logger, dtos.Metadata{}, conf.ManagerConfig{ImpressionsMode: conf.ImpressionsModeDebug}),
		TelemetryRecorder:  metric.NewRecorderSingle(metricStorageMock, splitAPI.MetricRecorder, dtos.Metadata{}),
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

	statusChannel := make(chan int64, 1)
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

	time.Sleep(1 * time.Second)
	if managerTest.status.Load().(int) != Streaming {
		t.Error("It should be Streaming")
	}
	managerTest.Stop()
	time.Sleep(1 * time.Second)

	if atomic.LoadInt64(&splitFetchCalled) != 4 {
		t.Error("It should be called fourth times")
	}
	if atomic.LoadInt64(&segmentFetchCalled) != 4 {
		t.Error("It should be called fourth times")
	}
}

/*
func TestMultipleErrors(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	advanced := conf.GetDefaultAdvancedConfig()

	streamingStatus := make(chan int64, 1)
	managerStatus := make(chan int64, 1)
	status := atomic.Value{}
	status.Store(Idle)

	var startCall int64
	var stopWorkersCall int64
	var startPeriodicFetchingCall int64
	var stopPeriodicFetchingCall int64
	var stopCall int64
	var startWorkersCall int64

	managerTest := Manager{
		synchronizer: syncMock.MockSynchronizer{
			SyncAllCall: func() error {
				return nil
			},
			StartPeriodicDataRecordingCall: func() {},
			StartPeriodicFetchingCall: func() {
				atomic.AddInt64(&startPeriodicFetchingCall, 1)
			},
			StopPeriodicFetchingCall: func() {
				atomic.AddInt64(&stopPeriodicFetchingCall, 1)
			},
			StopPeriodicDataRecordingCall: func() {},
		},
		config:        advanced,
		logger:        logger,
		managerStatus: managerStatus,
		pushManager: pushMock.MockManager{
			StartCall: func() {
				atomic.AddInt64(&startCall, 1)
			},
			StopWorkersCall: func() {
				atomic.AddInt64(&stopWorkersCall, 1)
			},
			StopCall: func() {
				atomic.AddInt64(&stopCall, 1)
				atomic.AddInt64(&stopWorkersCall, 1)
			},
			StartWorkersCall: func() {
				atomic.AddInt64(&startWorkersCall, 1)
			},
			IsRunningCall: func() bool {
				return true
			},
		},
		status:          status,
		streamingStatus: streamingStatus,
	}

	go managerTest.Start()
	<-managerStatus
	time.Sleep(100 * time.Millisecond)
	streamingStatus <- push.BackoffAuth
	time.Sleep(100 * time.Millisecond)
	if managerTest.status.Load() != Polling {
		t.Error("It should be running in Polling mode")
	}
	if atomic.LoadInt64(&startCall) != 1 || atomic.LoadInt64(&stopWorkersCall) != 1 || atomic.LoadInt64(&startPeriodicFetchingCall) != 1 || atomic.LoadInt64(&stopPeriodicFetchingCall) != 0 || atomic.LoadInt64(&stopCall) != 0 {
		t.Error("Unexpected state")
	}

	streamingStatus <- push.BackoffSSE
	// It should keep current state since is already in Polling mode
	if managerTest.status.Load() != Polling {
		t.Error("It should be running in Polling mode")
	}
	if atomic.LoadInt64(&startCall) != 1 || atomic.LoadInt64(&stopWorkersCall) != 1 || atomic.LoadInt64(&startPeriodicFetchingCall) != 1 || atomic.LoadInt64(&stopPeriodicFetchingCall) != 0 || atomic.LoadInt64(&stopCall) != 0 || atomic.LoadInt64(&startWorkersCall) != 0 {
		t.Error("Unexpected state")
	}

	streamingStatus <- push.Ready
	time.Sleep(100 * time.Millisecond)
	if managerTest.status.Load() != Streaming {
		t.Error("It should be running in Streaming mode")
	}
	if atomic.LoadInt64(&startCall) != 1 || atomic.LoadInt64(&startPeriodicFetchingCall) != 1 || atomic.LoadInt64(&stopWorkersCall) != 1 || atomic.LoadInt64(&stopPeriodicFetchingCall) != 1 || atomic.LoadInt64(&startWorkersCall) != 0 || atomic.LoadInt64(&stopCall) != 0 {
		t.Error("Unexpected state")
	}

	streamingStatus <- push.TokenExpiration
	time.Sleep(100 * time.Millisecond)
	if managerTest.status.Load() != Streaming {
		t.Error("It should be running in Streaming mode")
	}
	if atomic.LoadInt64(&stopCall) != 1 || atomic.LoadInt64(&startCall) != 2 || atomic.LoadInt64(&startPeriodicFetchingCall) != 1 || atomic.LoadInt64(&stopWorkersCall) != 2 || atomic.LoadInt64(&stopPeriodicFetchingCall) != 1 || atomic.LoadInt64(&startWorkersCall) != 0 {
		t.Error("Unexpected state")
	}

	streamingStatus <- push.PushIsDown
	time.Sleep(100 * time.Millisecond)
	if managerTest.status.Load() != Polling {
		t.Error("It should be running in Polling mode")
	}
	if atomic.LoadInt64(&stopCall) != 1 || atomic.LoadInt64(&startCall) != 2 || atomic.LoadInt64(&startPeriodicFetchingCall) != 2 || atomic.LoadInt64(&stopWorkersCall) != 3 || atomic.LoadInt64(&stopPeriodicFetchingCall) != 1 || atomic.LoadInt64(&startWorkersCall) != 0 {
		t.Error("Unexpected state")
	}

	streamingStatus <- push.PushIsUp
	time.Sleep(100 * time.Millisecond)
	if managerTest.status.Load() != Streaming {
		t.Error("It should be running in Streaming mode")
	}
	if atomic.LoadInt64(&stopCall) != 1 || atomic.LoadInt64(&startCall) != 2 || atomic.LoadInt64(&startPeriodicFetchingCall) != 2 || atomic.LoadInt64(&stopWorkersCall) != 3 || atomic.LoadInt64(&stopPeriodicFetchingCall) != 2 || atomic.LoadInt64(&startWorkersCall) != 1 {
		t.Error("Unexpected state")
	}

	streamingStatus <- push.Reconnect
	time.Sleep(100 * time.Millisecond)
	if managerTest.status.Load() != Streaming {
		t.Error("It should be running in Streaming mode")
	}
	if atomic.LoadInt64(&stopCall) != 2 || atomic.LoadInt64(&startCall) != 3 || atomic.LoadInt64(&startPeriodicFetchingCall) != 2 || atomic.LoadInt64(&stopWorkersCall) != 4 || atomic.LoadInt64(&stopPeriodicFetchingCall) != 2 || atomic.LoadInt64(&startWorkersCall) != 1 {
		t.Error("Unexpected state")
	}

	streamingStatus <- push.NonRetriableError
	time.Sleep(100 * time.Millisecond)
	if managerTest.status.Load() != Polling {
		t.Error("It should be running in Polling mode")
	}
	if atomic.LoadInt64(&stopCall) != 3 || atomic.LoadInt64(&startCall) != 3 || atomic.LoadInt64(&startPeriodicFetchingCall) != 3 || atomic.LoadInt64(&stopWorkersCall) != 6 || atomic.LoadInt64(&stopPeriodicFetchingCall) != 2 || atomic.LoadInt64(&startWorkersCall) != 1 {
		t.Error("Unexpected state")
	}

	managerTest.Stop()
	go managerTest.Start()
	<-managerStatus

	streamingStatus <- push.StreamingDisabled
	time.Sleep(100 * time.Millisecond)
	if managerTest.status.Load() != Polling {
		t.Error("It should be running in Polling mode")
	}
	if atomic.LoadInt64(&startPeriodicFetchingCall) != 4 || atomic.LoadInt64(&stopWorkersCall) != 9 {
		t.Error("Unexpected state")
	}
}
*/
