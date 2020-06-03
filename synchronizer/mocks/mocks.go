package mocks

// MockSynchronizer mock implementation
type MockSynchronizer struct {
	SyncAllCall                    func() error
	SynchronizeSplitsCall          func(till *int64) error
	SynchronizeSegmentCall         func(segmentName string, till *int64) error
	StartPeriodicFetchingCall      func()
	StopPeriodicFetchingCall       func()
	StartPeriodicDataRecordingCall func()
	StopPeriodicDataRecordingCall  func()
}

// SyncAll mock
func (m MockSynchronizer) SyncAll() error {
	return m.SyncAllCall()
}

// SynchronizeSplits mock
func (m MockSynchronizer) SynchronizeSplits(till *int64) error {
	return m.SynchronizeSplitsCall(till)
}

// SynchronizeSegment mock
func (m MockSynchronizer) SynchronizeSegment(segmentName string, till *int64) error {
	return m.SynchronizeSegmentCall(segmentName, till)
}

// StartPeriodicFetching mock
func (m MockSynchronizer) StartPeriodicFetching() {
	m.StartPeriodicFetchingCall()
}

// StopPeriodicFetching mock
func (m MockSynchronizer) StopPeriodicFetching() {
	m.StopPeriodicFetchingCall()
}

// StartPeriodicDataRecording mock
func (m MockSynchronizer) StartPeriodicDataRecording() {
	m.StartPeriodicDataRecordingCall()
}

// StopPeriodicDataRecording mock
func (m MockSynchronizer) StopPeriodicDataRecording() {
	m.StopPeriodicDataRecordingCall()
}
