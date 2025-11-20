package mocks

import (
	"time"

	"github.com/splitio/go-split-commons/v9/dtos"
)

// MockSynchronizer mock implementation
type MockSynchronizer struct {
	SyncAllCall                       func() error
	SynchronizeFeatureFlagsCall       func(ffChange *dtos.SplitChangeUpdate) error
	SynchronizeSegmentCall            func(segmentName string, till *int64) error
	StartPeriodicFetchingCall         func()
	StopPeriodicFetchingCall          func()
	StartPeriodicDataRecordingCall    func()
	StopPeriodicDataRecordingCall     func()
	LocalKillCall                     func(string, string, int64)
	RefreshRatesCall                  func() (time.Duration, time.Duration)
	SynchronizeLargeSegmentCall       func(name string, till *int64) error
	SynchronizeLargeSegmentUpdateCall func(lsRFDResponseDTO *dtos.LargeSegmentRFDResponseDTO) error
}

// SyncAll mock
func (m *MockSynchronizer) SyncAll() error {
	return m.SyncAllCall()
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
func (m *MockSynchronizer) SynchronizeFeatureFlags(ffChange *dtos.SplitChangeUpdate) error {
	return m.SynchronizeFeatureFlagsCall(ffChange)
}

// SynchronizeSegment mock
func (m *MockSynchronizer) SynchronizeLargeSegment(name string, till *int64) error {
	return m.SynchronizeLargeSegmentCall(name, till)
}

// SynchronizeLargeSegmentUpdate call
func (m *MockSynchronizer) SynchronizeLargeSegmentUpdate(lsRFDResponseDTO *dtos.LargeSegmentRFDResponseDTO) error {
	return m.SynchronizeLargeSegmentUpdateCall(lsRFDResponseDTO)
}
