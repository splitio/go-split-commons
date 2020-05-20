package synchronizer

import (
	"errors"

	"github.com/splitio/go-split-commons/service/api"
	"github.com/splitio/go-split-commons/storage"
)

// MetricSynchronizer struct for metric sync
type MetricSynchronizer struct {
	metricStorage  storage.MetricsStorage
	metricRecorder *api.HTTPMetricsRecorder
}

// NewMetricSynchronizer creates new metric synchronizer for posting metrics
func NewMetricSynchronizer(
	metricStorage storage.MetricsStorage,
	metricRecorder *api.HTTPMetricsRecorder,
) *MetricSynchronizer {
	return &MetricSynchronizer{
		metricStorage:  metricStorage,
		metricRecorder: metricRecorder,
	}
}

// SynchronizeLatencies syncs latencies
func (m *MetricSynchronizer) SynchronizeLatencies() error {
	latencies := m.metricStorage.PopLatencies()
	if len(latencies) > 0 {
		err := m.metricRecorder.RecordLatencies(latencies)
		return err
	}
	return nil
}

// SynchronizeGauges syncs gauges
func (m *MetricSynchronizer) SynchronizeGauges() error {
	var errs []error
	for _, gauge := range m.metricStorage.PopGauges() {
		err := m.metricRecorder.RecordGauge(gauge)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.New("Some gauges could not be posted")
	}
	return nil
}

// SynchronizeCounters syncs counters
func (m *MetricSynchronizer) SynchronizeCounters() error {
	counters := m.metricStorage.PopCounters()
	if len(counters) > 0 {
		err := m.metricRecorder.RecordCounters(counters)
		return err
	}
	return nil
}
