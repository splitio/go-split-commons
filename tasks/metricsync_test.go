package tasks

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/dtos"
	recorderMock "github.com/splitio/go-split-commons/service/mocks"
	storageMock "github.com/splitio/go-split-commons/storage/mocks"
	"github.com/splitio/go-split-commons/synchronizer"
	"github.com/splitio/go-toolkit/logging"
)

func TestCounterSyncTask(t *testing.T) {
	var popCounters int64
	var popLatency int64
	var popGauge int64

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
			atomic.AddInt64(&popCounters, 1)
			return nil
		},
		RecordGaugeCall: func(gauge dtos.GaugeDTO, metadata dtos.Metadata) error {
			atomic.AddInt64(&popGauge, 1)
			return nil
		},
		RecordLatenciesCall: func(latencies []dtos.LatenciesDTO, metadata dtos.Metadata) error {
			atomic.AddInt64(&popLatency, 1)
			return nil
		},
	}

	metricTask := NewRecordTelemetryTask(
		synchronizer.NewMetricSynchronizer(
			metricMockStorage,
			metricMockRecorder,
			dtos.Metadata{},
		),
		3,
		logging.NewLogger(&logging.LoggerOptions{}),
	)

	metricTask.Start()
	if !metricTask.IsRunning() {
		t.Error("Counter recorder task should be running")
	}

	metricTask.Stop(false)
	time.Sleep(time.Second * 1)
	if popCounters <= 0 {
		t.Error("Request not received")
	}
	if popGauge <= 0 {
		t.Error("Request not received")
	}
	if popLatency <= 0 {
		t.Error("Request not received")
	}
	if metricTask.IsRunning() {
		t.Error("Task should be stopped")
	}
}
