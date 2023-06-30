package mocks

import (
	"time"

	"github.com/splitio/go-split-commons/v4/dtos"
)

// MockSynchronizer mock implementation
type MockSynchronizer struct {
	SyncAllCall                           func() error
	SynchronizeSplitsCall                 func(till *int64) error
	SynchronizeFeatureFlagWithPayloadCall func(ffChange dtos.SplitChangeUpdate) error
	SynchronizeSegmentCall                func(segmentName string, till *int64) error
	StartPeriodicFetchingCall             func()
	StopPeriodicFetchingCall              func()
	StartPeriodicDataRecordingCall        func()
	StopPeriodicDataRecordingCall         func()
	LocalKillCall                         func(string, string, int64)
	RefreshRatesCall                      func() (time.Duration, time.Duration)
}

// SyncAll mock
func (m *MockSynchronizer) SyncAll() error {
	return m.SyncAllCall()
}

// SynchronizeSplits mock
func (m *MockSynchronizer) SynchronizeSplits(till *int64) error {
	return m.SynchronizeSplitsCall(till)
}

// SynchronizeSegment mock
func (m *MockSynchronizer) SynchronizeSegment(segmentName string, till *int64) error {
	return m.SynchronizeSegmentCall(segmentName, till)
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

// RefreshRates call
func (m *MockSynchronizer) SynchronizeFeatureFlagWithPayload(ffChange dtos.SplitChangeUpdate) error {
	return m.SynchronizeFeatureFlagWithPayloadCall(ffChange)
}
