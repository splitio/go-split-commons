package mocks

type LocalSyncMock struct {
	SyncAllCall                    func(requestNoCache bool) error
	SynchronizeSplitsCall          func(till *int64) error
	LocalKillCall                  func(splitName string, defaultTreatment string, changeNumber int64)
	SynchronizeSegmentCall         func(segmentName string, till *int64, requestNoCache bool) error
	StartPeriodicFetchingCall      func()
	StopPeriodicFetchingCall       func()
	StartPeriodicDataRecordingCall func()
	StopPeriodicDataRecordingCall  func()
}

func (l *LocalSyncMock) SyncAll(requestNoCache bool) error {
	return l.SyncAllCall(requestNoCache)
}

func (l *LocalSyncMock) SynchronizeSplits(till *int64) error {
	return l.SynchronizeSplitsCall(till)
}

func (l *LocalSyncMock) LocalKill(splitName string, defaultTreatment string, changeNumber int64) {
	l.LocalKillCall(splitName, defaultTreatment, changeNumber)
}

func (l *LocalSyncMock) SynchronizeSegment(segmentName string, till *int64, requestNoCache bool) error {
	return l.SynchronizeSegmentCall(segmentName, till, requestNoCache)
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
