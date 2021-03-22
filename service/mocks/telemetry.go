package mocks

import "github.com/splitio/go-split-commons/v3/dtos"

// MockTelemetryRecorder mocked implementation of telemetry recorder
type MockTelemetryRecorder struct {
	RecordInitCall  func(init dtos.Init, metadata dtos.Metadata) error
	RecordStatsCall func(stats dtos.Stats, metadata dtos.Metadata) error
}

// RecordInit mock
func (m MockTelemetryRecorder) RecordInit(init dtos.Init, metadata dtos.Metadata) error {
	return m.RecordInitCall(init, metadata)
}

// RecordStats mock
func (m MockTelemetryRecorder) RecordStats(stats dtos.Stats, metadata dtos.Metadata) error {
	return m.RecordStatsCall(stats, metadata)
}
