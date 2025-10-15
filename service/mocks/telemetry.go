package mocks

import "github.com/splitio/go-split-commons/v7/dtos"

// MockTelemetryRecorder mocked implementation of telemetry recorder
type MockTelemetryRecorder struct {
	RecordConfigCall     func(config dtos.Config, metadata dtos.Metadata) error
	RecordStatsCall      func(stats dtos.Stats, metadata dtos.Metadata) error
	RecordUniqueKeysCall func(uniques dtos.Uniques, metadata dtos.Metadata) error
}

// RecordConfig mock
func (m MockTelemetryRecorder) RecordConfig(config dtos.Config, metadata dtos.Metadata) error {
	return m.RecordConfigCall(config, metadata)
}

// RecordStats mock
func (m MockTelemetryRecorder) RecordStats(stats dtos.Stats, metadata dtos.Metadata) error {
	return m.RecordStatsCall(stats, metadata)
}

// RecordUniqueKeys mock
func (m MockTelemetryRecorder) RecordUniqueKeys(uniques dtos.Uniques, metadata dtos.Metadata) error {
	return m.RecordUniqueKeysCall(uniques, metadata)
}
