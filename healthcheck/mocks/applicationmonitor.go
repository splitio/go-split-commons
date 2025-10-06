package mocks

import (
	"github.com/splitio/go-split-commons/v7/healthcheck/application"

	"github.com/stretchr/testify/mock"
)

// MockApplicationMonitor mocked implementation of application monitor
type MockApplicationMonitor struct {
	NotifyEventCall func(counterType int)
	ResetCall       func(counterType int, value int)
}

// NotifyEvent mock
func (m MockApplicationMonitor) NotifyEvent(counterType int) {
	m.NotifyEventCall(counterType)
}

// Reset mock
func (m MockApplicationMonitor) Reset(counterType int, value int) {
	m.ResetCall(counterType, value)
}

// ApplicationMonitorMock is a mock for the ApplicationMonitor interface
type ApplicationMonitorMock struct {
	mock.Mock
}

// NotifyEvent mock
func (m *ApplicationMonitorMock) NotifyEvent(counterType int) {
	m.Called(counterType)
}

// Reset mock
func (m *ApplicationMonitorMock) Reset(counterType int, value int) {
	m.Called(counterType, value)
}

var _ application.MonitorProducerInterface = (*MockApplicationMonitor)(nil)
