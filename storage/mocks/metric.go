package mocks

import "github.com/splitio/go-split-commons/dtos"

// MockMetricStorage is a mocked implementation of Metric Storage
type MockMetricStorage struct {
	IncCounterCall   func(key string)
	IncLatencyCall   func(metricName string, index int)
	PutGaugeCall     func(key string, gauge float64)
	PopGaugesCall    func() []dtos.GaugeDTO
	PopLatenciesCall func() []dtos.LatenciesDTO
	PopCountersCall  func() []dtos.CounterDTO
}

// IncCounter mock
func (m MockMetricStorage) IncCounter(key string) {
	m.IncCounterCall(key)
}

// IncLatency mock
func (m MockMetricStorage) IncLatency(metricName string, index int) {
	m.IncLatencyCall(metricName, index)
}

// PutGauge mock
func (m MockMetricStorage) PutGauge(key string, gauge float64) {
	m.PutGaugeCall(key, gauge)
}

// PopGauges mock
func (m MockMetricStorage) PopGauges() []dtos.GaugeDTO {
	return m.PopGaugesCall()
}

// PopLatencies mock
func (m MockMetricStorage) PopLatencies() []dtos.LatenciesDTO {
	return m.PopLatenciesCall()
}

// PopCounters mock
func (m MockMetricStorage) PopCounters() []dtos.CounterDTO {
	return m.PopCountersCall()
}
