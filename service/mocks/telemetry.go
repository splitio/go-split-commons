package mocks

import "github.com/splitio/go-split-commons/v3/dtos"

// MockTelemetryRecorder mocked implementation of telemetry recorder
type MockTelemetryRecorder struct {
	RecordStatsCall func(stats dtos.Stats, metadata dtos.Metadata) error
}

// Record mock
func (m MockTelemetryRecorder) RecordStats(stats dtos.Stats, metadata dtos.Metadata) error {
	return m.RecordStatsCall(stats, metadata)
}
