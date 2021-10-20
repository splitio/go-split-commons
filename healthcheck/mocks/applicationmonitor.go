package mocks

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
