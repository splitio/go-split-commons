package mocks

import "time"

// MockSynchronizer mock implementation
type MockSynchronizer struct {
	SyncAllCall                    func(bool) error
	SynchronizeSplitsCall          func(till *int64, requestNoCache bool) error
	SynchronizeSegmentCall         func(segmentName string, till *int64, requestNoCache bool) error
	StartPeriodicFetchingCall      func()
	StopPeriodicFetchingCall       func()
	StartPeriodicDataRecordingCall func()
	StopPeriodicDataRecordingCall  func()
	LocalKillCall                  func(string, string, int64)
	RefreshRatesCall               func() (time.Duration, time.Duration)
}

// SyncAll mock
func (m *MockSynchronizer) SyncAll(requestNoCache bool) error {
	return m.SyncAllCall(requestNoCache)
}

// SynchronizeSplits mock
func (m *MockSynchronizer) SynchronizeSplits(till *int64, requestNoCache bool) error {
	return m.SynchronizeSplitsCall(till, requestNoCache)
}

// SynchronizeSegment mock
func (m *MockSynchronizer) SynchronizeSegment(segmentName string, till *int64, requestNoCache bool) error {
	return m.SynchronizeSegmentCall(segmentName, till, requestNoCache)
}

// StartPeriodicFetching mock
func (m *MockSynchronizer) StartPeriodicFetching() {
	m.StartPeriodicFetchingCall()
}

// StopPeriodicFetching mock
func (m *MockSynchronizer) StopPeriodicFetching() {
	m.StopPeriodicFetchingCall()
}

// StartPeriodicDataRecording mock
func (m *MockSynchronizer) StartPeriodicDataRecording() {
	m.StartPeriodicDataRecordingCall()
}

// StopPeriodicDataRecording mock
func (m *MockSynchronizer) StopPeriodicDataRecording() {
	m.StopPeriodicDataRecordingCall()
}

// LocalKill mock
func (m *MockSynchronizer) LocalKill(name string, treatment string, cn int64) {
	m.LocalKillCall(name, treatment, cn)
}

// RefreshRates call
func (m *MockSynchronizer) RefreshRates() (time.Duration, time.Duration) {
	return m.RefreshRatesCall()
}
