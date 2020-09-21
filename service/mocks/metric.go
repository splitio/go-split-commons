package mocks

import "github.com/splitio/go-split-commons/v2/dtos"

// MockMetricRecorder mocked implementation of metric recorder
type MockMetricRecorder struct {
	RecordCountersCall  func(counters []dtos.CounterDTO, metadata dtos.Metadata) error
	RecordLatenciesCall func(latencies []dtos.LatenciesDTO, metadata dtos.Metadata) error
	RecordGaugeCall     func(gauge dtos.GaugeDTO, metadata dtos.Metadata) error
}

// RecordCounters mock
func (m MockMetricRecorder) RecordCounters(counters []dtos.CounterDTO, metadata dtos.Metadata) error {
	return m.RecordCountersCall(counters, metadata)
}

// RecordLatencies mock
func (m MockMetricRecorder) RecordLatencies(latencies []dtos.LatenciesDTO, metadata dtos.Metadata) error {
	return m.RecordLatenciesCall(latencies, metadata)
}

// RecordGauge mock
func (m MockMetricRecorder) RecordGauge(gauge dtos.GaugeDTO, metadata dtos.Metadata) error {
	return m.RecordGaugeCall(gauge, metadata)
}
