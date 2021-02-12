package synchronizer

// Synchronizer interface for syncing data to and from splits servers
type Synchronizer interface {
	SyncAll() error
	SynchronizeSplits(till *int64) error
	LocalKill(splitName string, defaultTreatment string, changeNumber int64)
	SynchronizeSegment(segmentName string, till *int64) error
	StartPeriodicFetching()
	StopPeriodicFetching()
	StartPeriodicDataRecording()
	StopPeriodicDataRecording()
}
