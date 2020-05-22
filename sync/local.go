package sync

import (
	"github.com/splitio/go-split-commons/service"
	"github.com/splitio/go-split-commons/storage"
	"github.com/splitio/go-split-commons/synchronizer"
	"github.com/splitio/go-split-commons/tasks"
	"github.com/splitio/go-toolkit/logging"
)

// LocalSynchronizerImpl implements Synchronizer
type LocalSynchronizerImpl struct {
	splitTasks          splitTasks
	synchronizers       synchronizers
	logger              logging.LoggerInterface
	inMememoryFullQueue chan string
}

// NewLocalSynchronizerImpl creates new SynchronizerImpl
func NewLocalSynchronizerImpl(
	period int,
	splitAPI *service.SplitAPI,
	splitStorage storage.SplitStorage,
	logger logging.LoggerInterface,
) Synchronizer {
	synchronizers := synchronizers{
		splitSynchronizer: synchronizer.NewSplitSynchronizer(splitStorage, splitAPI.SplitFetcher),
	}
	return &SynchronizerImpl{
		splitTasks: splitTasks{
			splitSyncTask: tasks.NewFetchSplitsTask(synchronizers.splitSynchronizer, period, logger),
		},
		synchronizers: synchronizers,
		logger:        logger,
	}
}

// SyncAll syncs splits and segments
func (s *LocalSynchronizerImpl) SyncAll() error {
	return s.synchronizers.splitSynchronizer.SynchronizeSplits(nil)
}

// StartPeriodicFetching starts periodic fetchers tasks
func (s *LocalSynchronizerImpl) StartPeriodicFetching() {
	s.splitTasks.splitSyncTask.Start()
}

// StopPeriodicFetching stops periodic fetchers tasks
func (s *LocalSynchronizerImpl) StopPeriodicFetching() {
	s.splitTasks.splitSyncTask.Stop(false)
}

// StartPeriodicDataRecording starts periodic recorders tasks
func (s *LocalSynchronizerImpl) StartPeriodicDataRecording() {
}

// StopPeriodicDataRecording stops periodic recorders tasks
func (s *LocalSynchronizerImpl) StopPeriodicDataRecording() {
}

// SynchronizeSplits syncs splits
func (s *LocalSynchronizerImpl) SynchronizeSplits(till *int64) error {
	return s.synchronizers.splitSynchronizer.SynchronizeSplits(till)
}

// SynchronizeSegment syncs segment
func (s *LocalSynchronizerImpl) SynchronizeSegment(name string, till *int64) error {
	return nil
}
