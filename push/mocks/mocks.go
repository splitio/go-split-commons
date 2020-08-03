package mocks

// MockManager mocking struct for push
type MockManager struct {
	StartCall        func()
	StopCall         func()
	StartWorkersCall func()
	StopWorkersCall  func()
	IsRunningCall    func() bool
}

// Start mock
func (m MockManager) Start() {
	m.StartCall()
}

// Stop mock
func (m MockManager) Stop() {
	m.StopCall()
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
