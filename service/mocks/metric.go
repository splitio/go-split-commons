package mocks

import "github.com/splitio/go-split-commons/dtos"

// MockMetricRecorder mocked implementation of metric recorder
type MockMetricRecorder struct {
	RecordCountersCall  func(counters []dtos.CounterDTO) error
	RecordLatenciesCall func(latencies []dtos.LatenciesDTO) error
	RecordGaugeCall     func(gauge dtos.GaugeDTO) error
}

// RecordCounters mock
func (m MockMetricRecorder) RecordCounters(counters []dtos.CounterDTO) error {
	return m.RecordCountersCall(counters)
}

// RecordLatencies mock
func (m MockMetricRecorder) RecordLatencies(latencies []dtos.LatenciesDTO) error {
	return m.RecordLatenciesCall(latencies)
}

// RecordGauge mock
func (m MockMetricRecorder) RecordGauge(gauge dtos.GaugeDTO) error {
	return m.RecordGaugeCall(gauge)
}
