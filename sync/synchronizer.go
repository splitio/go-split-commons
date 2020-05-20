package sync

import (
	"github.com/splitio/go-split-commons/synchronizer"
	"github.com/splitio/go-split-commons/tasks"
	"github.com/splitio/go-toolkit/logging"
)

// Synchronizer interface for syncing data to and from splits servers
type Synchronizer interface {
	SyncAll() bool
	SynchronizeSplits(till *int64) (bool, error)
	SynchronizeSegment(segmentName string, till *int64) (bool, error)
	StartPeriodicFetching()
	StopPeriodicFetching()
	StartPeriodicDataRecording()
	StopPeriodicDataRecording()
}

// SynchronizerImpl implements Synchronizer
type SynchronizerImpl struct {
	splitTasks    tasks.SplitTasks
	synchronizers synchronizer.SplitSynchronizers
}

// NewSynchronizerImpl creates new SynchronizerImpl
func NewSynchronizerImpl(
	synchronizers synchronizer.SplitSynchronizers,
	splitTasks tasks.SplitTasks,
	logger logging.LoggerInterface,
) Synchronizer {
	return &SynchronizerImpl{
		splitTasks:    splitTasks,
		synchronizers: synchronizers,
	}
}

// SyncAll syncs splits and segments
func (s *SynchronizerImpl) SyncAll() bool {
	done, err := s.synchronizers.SplitSynchronizer.SynchronizeSplits(nil)
	if err != nil || !done {
		return false
	}
	done, err = s.synchronizers.SegmentSynchronizer.SynchronizeSegments()
	if err != nil {
		return false
	}
	return done
}

// StartPeriodicFetching starts periodic fetchers tasks
func (s *SynchronizerImpl) StartPeriodicFetching() {
	s.splitTasks.SplitTask.Start()
	s.splitTasks.SegmentSyncTask.Start()
}

// StopPeriodicFetching stops periodic fetchers tasks
func (s *SynchronizerImpl) StopPeriodicFetching() {
	s.splitTasks.SplitTask.Stop(false)
	s.splitTasks.SegmentSyncTask.Stop(false)
}

// StartPeriodicDataRecording starts periodic recorders tasks
func (s *SynchronizerImpl) StartPeriodicDataRecording() {
	s.splitTasks.ImpressionSyncTask.Start()
	s.splitTasks.LatencySyncTask.Start()
	s.splitTasks.GaugeSyncTask.Start()
	s.splitTasks.CounterSyncTask.Start()
	s.splitTasks.EventSyncTask.Start()
}

// StopPeriodicDataRecording stops periodic recorders tasks
func (s *SynchronizerImpl) StopPeriodicDataRecording() {
	s.splitTasks.ImpressionSyncTask.Stop(true)
	s.splitTasks.LatencySyncTask.Stop(true)
	s.splitTasks.GaugeSyncTask.Stop(true)
	s.splitTasks.CounterSyncTask.Stop(true)
	s.splitTasks.EventSyncTask.Stop(true)
}

// SynchronizeSplits syncs splits
func (s *SynchronizerImpl) SynchronizeSplits(till *int64) (bool, error) {
	return s.synchronizers.SplitSynchronizer.SynchronizeSplits(till)
}

// SynchronizeSegment syncs segment
func (s *SynchronizerImpl) SynchronizeSegment(name string, till *int64) (bool, error) {
	return s.synchronizers.SegmentSynchronizer.SynchronizeSegment(name, till)
}
