package telemetry

// SynchronizeEvents interface
type TelemetryRecorder interface {
	SynchronizeTelemetry() error
}
