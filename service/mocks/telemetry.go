package mocks

import "github.com/splitio/go-split-commons/v4/dtos"

// MockTelemetryRecorder mocked implementation of telemetry recorder
type MockTelemetryRecorder struct {
	RecordConfigCall func(config dtos.Config, metadata dtos.Metadata) error
	RecordStatsCall  func(stats dtos.Stats, metadata dtos.Metadata) error
}

// RecordConfig mock
func (m MockTelemetryRecorder) RecordConfig(config dtos.Config, metadata dtos.Metadata) error {
	return m.RecordConfigCall(config, metadata)
}

// RecordStats mock
func (m MockTelemetryRecorder) RecordStats(stats dtos.Stats, metadata dtos.Metadata) error {
	return m.RecordStatsCall(stats, metadata)
}
