package mocks

import (
	"github.com/splitio/go-split-commons/v5/dtos"
)

type LocalSyncMock struct {
	SyncAllCall                    func() error
	SynchronizeFeatureFlagsCall    func(ffChange *dtos.SplitChangeUpdate) error
	LocalKillCall                  func(splitName string, defaultTreatment string, changeNumber int64)
	SynchronizeSegmentCall         func(segmentName string, till *int64) error
	StartPeriodicFetchingCall      func()
	StopPeriodicFetchingCall       func()
	StartPeriodicDataRecordingCall func()
	StopPeriodicDataRecordingCall  func()
}

func (l *LocalSyncMock) SyncAll() error {
	return l.SyncAllCall()
}

func (l *LocalSyncMock) LocalKill(splitName string, defaultTreatment string, changeNumber int64) {
	l.LocalKillCall(splitName, defaultTreatment, changeNumber)
}

func (l *LocalSyncMock) SynchronizeSegment(segmentName string, till *int64) error {
	return l.SynchronizeSegmentCall(segmentName, till)
}

func (l *LocalSyncMock) StartPeriodicFetching() {
	l.StartPeriodicFetchingCall()
}

func (l *LocalSyncMock) StopPeriodicFetching() {
	l.StopPeriodicFetchingCall()
}

func (l *LocalSyncMock) StartPeriodicDataRecording() {
	l.StartPeriodicDataRecordingCall()
}

func (l *LocalSyncMock) StopPeriodicDataRecording() {
	l.StopPeriodicDataRecordingCall()
}

func (l *LocalSyncMock) SynchronizeFeatureFlags(ffChange *dtos.SplitChangeUpdate) error {
	return l.SynchronizeFeatureFlagsCall(ffChange)
}
