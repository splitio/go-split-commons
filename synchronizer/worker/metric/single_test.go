package metric

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service/api"
	recorderMock "github.com/splitio/go-split-commons/service/mocks"
	storageMock "github.com/splitio/go-split-commons/storage/mocks"
	"github.com/splitio/go-split-commons/storage/mutexmap"
	"github.com/splitio/go-toolkit/logging"
)

func TestMetricSynchronizerError(t *testing.T) {
	metricMockStorage := storageMock.MockMetricStorage{
		PopCountersCall: func() []dtos.CounterDTO {
			toReturn := make([]dtos.CounterDTO, 0, 1)
			toReturn = append(toReturn, dtos.CounterDTO{
				MetricName: "counter",
				Count:      1,
			})
			return toReturn
		},
		PopGaugesCall: func() []dtos.GaugeDTO {
			toReturn := make([]dtos.GaugeDTO, 0, 1)
			toReturn = append(toReturn, dtos.GaugeDTO{
				MetricName: "gauge",
				Gauge:      1,
			})
			return toReturn
		},
		PopLatenciesCall: func() []dtos.LatenciesDTO {
			toReturn := make([]dtos.LatenciesDTO, 0, 1)
			toReturn = append(toReturn, dtos.LatenciesDTO{
				MetricName: "latency",
				Latencies:  []int64{1, 2, 3},
			})
			return toReturn
		},
	}

	metricSync := NewRecorderSingle(
		metricMockStorage,
		recorderMock.MockMetricRecorder{
			RecordCountersCall: func(counters []dtos.CounterDTO, metadata dtos.Metadata) error {
				return errors.New("some")
			},
			RecordGaugeCall: func(gauge dtos.GaugeDTO, metadata dtos.Metadata) error {
				return errors.New("some")
			},
			RecordLatenciesCall: func(latencies []dtos.LatenciesDTO, metadata dtos.Metadata) error {
				return errors.New("some")
			},
		},
		dtos.Metadata{},
	)

	err := metricSync.SynchronizeTelemetry()
	if err == nil {
		t.Error("It should return err")
	}

	metricSync2 := NewRecorderSingle(
		metricMockStorage,
		recorderMock.MockMetricRecorder{
			RecordCountersCall: func(counters []dtos.CounterDTO, metadata dtos.Metadata) error {
				return nil
			},
			RecordGaugeCall: func(gauge dtos.GaugeDTO, metadata dtos.Metadata) error {
				return errors.New("some")
			},
			RecordLatenciesCall: func(latencies []dtos.LatenciesDTO, metadata dtos.Metadata) error {
				return errors.New("some")
			},
		},
		dtos.Metadata{},
	)

	err = metricSync2.SynchronizeTelemetry()
	if err == nil {
		t.Error("It should return err")
	}

	metricSync3 := NewRecorderSingle(
		metricMockStorage,
		recorderMock.MockMetricRecorder{
			RecordCountersCall: func(counters []dtos.CounterDTO, metadata dtos.Metadata) error {
				return nil
			},
			RecordGaugeCall: func(gauge dtos.GaugeDTO, metadata dtos.Metadata) error {
				return nil
			},
			RecordLatenciesCall: func(latencies []dtos.LatenciesDTO, metadata dtos.Metadata) error {
				return errors.New("some")
			},
		},
		dtos.Metadata{},
	)

	err = metricSync3.SynchronizeTelemetry()
	if err == nil {
		t.Error("It should return err")
	}
}

func TestMetricSynchronizer(t *testing.T) {
	metricMockStorage := storageMock.MockMetricStorage{
		PopCountersCall: func() []dtos.CounterDTO {
			toReturn := make([]dtos.CounterDTO, 0, 1)
			toReturn = append(toReturn, dtos.CounterDTO{
				MetricName: "counter",
				Count:      1,
			})
			return toReturn
		},
		PopGaugesCall: func() []dtos.GaugeDTO {
			toReturn := make([]dtos.GaugeDTO, 0, 1)
			toReturn = append(toReturn, dtos.GaugeDTO{
				MetricName: "gauge",
				Gauge:      1,
			})
			return toReturn
		},
		PopLatenciesCall: func() []dtos.LatenciesDTO {
			toReturn := make([]dtos.LatenciesDTO, 0, 1)
			toReturn = append(toReturn, dtos.LatenciesDTO{
				MetricName: "latency",
				Latencies:  []int64{1, 2, 3},
			})
			return toReturn
		},
	}

	metricMockRecorder := recorderMock.MockMetricRecorder{
		RecordCountersCall: func(counters []dtos.CounterDTO, metadata dtos.Metadata) error {
			if len(counters) != 1 {
				t.Error("Wrong length for counters")
			}
			if counters[0].MetricName != "counter" || counters[0].Count != 1 {
				t.Error("Wrong counter passed")
			}
			return nil
		},
		RecordGaugeCall: func(gauge dtos.GaugeDTO, metadata dtos.Metadata) error {
			if gauge.MetricName != "gauge" {
				t.Error("Wrong gauge")
			}
			return nil
		},
		RecordLatenciesCall: func(latencies []dtos.LatenciesDTO, metadata dtos.Metadata) error {
			if len(latencies) != 1 {
				t.Error("Wrong length for counters")
			}
			if latencies[0].MetricName != "latency" || len(latencies[0].Latencies) != 3 {
				t.Error("Wrong latency passed")
			}
			return nil
		},
	}

	metricSync := NewRecorderSingle(
		metricMockStorage,
		metricMockRecorder,
		dtos.Metadata{},
	)

	err := metricSync.SynchronizeTelemetry()
	if err != nil {
		t.Error("It should not return err")
	}
}

func TestMetricSyncProcess(t *testing.T) {
	var countersRequestReceived int64
	var gaugesRequestReceived int64
	var latenciesRequestReceived int64

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Error("Method should be POST")
		}

		body, err := ioutil.ReadAll(r.Body)
		r.Body.Close()
		if err != nil {
			t.Error("Error reading body")
			return
		}

		switch r.URL.Path {
		case "/metrics/times":
			atomic.AddInt64(&latenciesRequestReceived, 1)
			var latencies []dtos.LatenciesDTO
			err = json.Unmarshal(body, &latencies)
			if err != nil {
				t.Errorf("Error parsing json: %s", err)
				return
			}
			if latencies[0].MetricName != "metric1" {
				t.Error("Incorrect metric name")
			}
		case "/metrics/counters":
			atomic.AddInt64(&countersRequestReceived, 1)
			var counters []dtos.CounterDTO
			err = json.Unmarshal(body, &counters)
			if err != nil {
				t.Errorf("Error parsing json: %s", err)
				return
			}
			if counters[0].MetricName != "counter1" || counters[0].Count != 1 {
				t.Error("Incorrect count received")
			}
		case "/metrics/gauge":
			atomic.AddInt64(&gaugesRequestReceived, 1)
			var gauge dtos.GaugeDTO
			err = json.Unmarshal(body, &gauge)
			if err != nil {
				t.Errorf("Error parsing json: %s", err)
				return
			}
			if gauge.MetricName != "g1" || gauge.Gauge != 123 {
				t.Error("Incorrect gauge received")
			}
		default:
			t.Errorf("Incorrect url %s", r.URL.Path)
		}

	}))
	defer ts.Close()

	logger := logging.NewLogger(&logging.LoggerOptions{})
	metricRecorder := api.NewHTTPMetricsRecorder(
		"",
		conf.AdvancedConfig{
			EventsURL: ts.URL,
			SdkURL:    ts.URL,
		},
		logger,
	)

	metricStorage := mutexmap.NewMMMetricsStorage()

	metricStorage.PutGauge("g1", 123)
	metricStorage.IncLatency("metric1", 5)
	metricStorage.IncCounter("counter1")

	metricSync := NewRecorderSingle(
		metricStorage,
		metricRecorder,
		dtos.Metadata{},
	)

	err := metricSync.SynchronizeTelemetry()
	if err != nil {
		t.Error("It should not return err")
	}

	if latenciesRequestReceived != 1 {
		t.Error("It should call once")
	}
	if gaugesRequestReceived != 1 {
		t.Error("It should call once")
	}
	if countersRequestReceived != 1 {
		t.Error("It should call once")
	}
}
