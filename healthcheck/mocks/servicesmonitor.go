package mocks

import "github.com/splitio/go-split-commons/v4/healthcheck/services"

// MockServicesMonitor mocked implementation of services monitor
type MockServicesMonitor struct {
	StartCall           func()
	StopCall            func()
	GetHealthStatusCall func() services.HealthDto
}

// Start mock
func (m MockServicesMonitor) Start() {
	m.StartCall()
}

// Stop mock
func (m MockServicesMonitor) Stop() {
	m.StopCall()
}

// GetHealthStatus mock
func (m MockServicesMonitor) GetHealthStatus() services.HealthDto {
	return m.GetHealthStatusCall()
}
