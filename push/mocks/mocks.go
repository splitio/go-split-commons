package mocks

import "time"

// MockManager mocking struct for push
type MockManager struct {
	StartCall        func() error
	StopCall         func() error
	StartWorkersCall func()
	StopWorkersCall  func()
	IsRunningCall    func() bool
	NextRefreshCall  func() time.Time
}

// Start mock
func (m MockManager) Start() error {
	return m.StartCall()
}

// Stop mock
func (m MockManager) Stop() error {
	return m.StopCall()
}

// StartWorkers mock
func (m MockManager) StartWorkers() {
	m.StartWorkersCall()
}

// StopWorkers mock
func (m MockManager) StopWorkers() {
	m.StopWorkersCall()
}

// IsRunning mock
func (m MockManager) IsRunning() bool {
	return m.IsRunningCall()
}

// NextRefresh call
func (m MockManager) NextRefresh() time.Time {
	return m.NextRefreshCall()
}
