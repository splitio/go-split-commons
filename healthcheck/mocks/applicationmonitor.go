package mocks

import (
	"github.com/splitio/go-split-commons/v4/healthcheck/application"
)

// MockApplicationMonitor mocked implementation of application monitor
type MockApplicationMonitor struct {
	StartCall           func()
	StopCall            func()
	GetHealthStatusCall func() application.HealthDto
	NotifyEventCall     func(counterType int)
	ResetCall           func(counterType int, value int)
}

// Start mock
func (m MockApplicationMonitor) Start() {
	m.StartCall()
}

// Stop mock
func (m MockApplicationMonitor) Stop() {
	m.StopCall()
}

// GetHealthStatus mock
func (m MockApplicationMonitor) GetHealthStatus() application.HealthDto {
	return m.GetHealthStatus()
}

// NotifyEvent mock
func (m MockApplicationMonitor) NotifyEvent(counterType int) {
	m.NotifyEventCall(counterType)
}

// Reset mock
func (m MockApplicationMonitor) Reset(counterType int, value int) {
	m.ResetCall(counterType, value)
}
