package worker

import (
	"errors"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service"
	"github.com/splitio/go-split-commons/storage"
)

// MetricRecorder struct for metric sync
type MetricRecorder struct {
	metricStorage  storage.MetricsStorageConsumer
	metricRecorder service.MetricsRecorder
	metadata       dtos.Metadata
}

// NewMetricRecorder creates new metric synchronizer for posting metrics
func NewMetricRecorder(
	metricStorage storage.MetricsStorageConsumer,
	metricRecorder service.MetricsRecorder,
	metadata dtos.Metadata,
) *MetricRecorder {
	return &MetricRecorder{
		metricStorage:  metricStorage,
		metricRecorder: metricRecorder,
		metadata:       metadata,
	}
}

func (m *MetricRecorder) synchronizeLatencies() error {
	latencies := m.metricStorage.PopLatencies()
	if len(latencies) > 0 {
		err := m.metricRecorder.RecordLatencies(latencies, m.metadata)
		return err
	}
	return nil
}

func (m *MetricRecorder) synchronizeGauges() error {
	var errs []error
	for _, gauge := range m.metricStorage.PopGauges() {
		err := m.metricRecorder.RecordGauge(gauge, m.metadata)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.New("Some gauges could not be posted")
	}
	return nil
}

func (m *MetricRecorder) synchronizeCounters() error {
	counters := m.metricStorage.PopCounters()
	if len(counters) > 0 {
		err := m.metricRecorder.RecordCounters(counters, m.metadata)
		return err
	}
	return nil
}

// SynchronizeTelemetry syncs telemetry
func (m *MetricRecorder) SynchronizeTelemetry() error {
	err := m.synchronizeGauges()
	if err != nil {
		return err
	}
	err = m.synchronizeLatencies()
	if err != nil {
		return err
	}
	return m.synchronizeCounters()
}
