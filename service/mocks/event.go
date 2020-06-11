package mocks

import "github.com/splitio/go-split-commons/dtos"

// MockEventRecorder mocked implementation of event recorder
type MockEventRecorder struct {
	RecordCall func(events []dtos.EventDTO, metadata dtos.Metadata) error
}

// Record mock
func (m MockEventRecorder) Record(events []dtos.EventDTO, metadata dtos.Metadata) error {
	return m.RecordCall(events, metadata)
}
