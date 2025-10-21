package push

import "github.com/splitio/go-split-commons/v8/dtos"

// Borrowed synchronizer interface to break circular dependencies
type synchronizerInterface interface {
	SyncAll() error
	SynchronizeFeatureFlags(ffChange *dtos.SplitChangeUpdate) error
	LocalKill(splitName string, defaultTreatment string, changeNumber int64)
	SynchronizeSegment(segmentName string, till *int64) error
	StartPeriodicFetching()
	StopPeriodicFetching()
	StartPeriodicDataRecording()
	StopPeriodicDataRecording()
	SynchronizeLargeSegment(name string, till *int64) error
	SynchronizeLargeSegmentUpdate(lsRFDResponseDTO *dtos.LargeSegmentRFDResponseDTO) error
}
