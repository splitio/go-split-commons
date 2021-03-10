package synchronizer

import (
	"github.com/splitio/go-split-commons/v3/service"
	"github.com/splitio/go-split-commons/v3/storage"
	"github.com/splitio/go-split-commons/v3/synchronizer/worker/split"
	"github.com/splitio/go-split-commons/v3/tasks"
	"github.com/splitio/go-toolkit/v4/logging"
)

// Local implements Local Synchronizer
type Local struct {
	splitTasks          SplitTasks
	workers             Workers
	logger              logging.LoggerInterface
	inMememoryFullQueue chan string
}

// NewLocal creates new Local
func NewLocal(
	period int,
	splitAPI *service.SplitAPI,
	splitStorage storage.SplitStorage,
	logger logging.LoggerInterface,
) Synchronizer {
	workers := Workers{
		SplitFetcher: split.NewSplitFetcher(splitStorage, splitAPI.SplitFetcher, logger),
	}
	return &Local{
		splitTasks: SplitTasks{
			SplitSyncTask: tasks.NewFetchSplitsTask(workers.SplitFetcher, period, logger),
		},
		workers: workers,
		logger:  logger,
	}
}

// SyncAll syncs splits and segments
func (s *Local) SyncAll(requestNoCache bool) error {
	_, err := s.workers.SplitFetcher.SynchronizeSplits(nil, requestNoCache)
	return err
}

// StartPeriodicFetching starts periodic fetchers tasks
func (s *Local) StartPeriodicFetching() {
	s.splitTasks.SplitSyncTask.Start()
}

// StopPeriodicFetching stops periodic fetchers tasks
func (s *Local) StopPeriodicFetching() {
	s.splitTasks.SplitSyncTask.Stop(false)
}

// StartPeriodicDataRecording starts periodic recorders tasks
func (s *Local) StartPeriodicDataRecording() {
}

// StopPeriodicDataRecording stops periodic recorders tasks
func (s *Local) StopPeriodicDataRecording() {
}

// SynchronizeSplits syncs splits
func (s *Local) SynchronizeSplits(till *int64, requestNoCache bool) error {
	_, err := s.workers.SplitFetcher.SynchronizeSplits(nil, requestNoCache)
	return err
}

// SynchronizeSegment syncs segment
func (s *Local) SynchronizeSegment(name string, till *int64, _ bool) error {
	return nil
}

// LocalKill does nothing
func (s *Local) LocalKill(splitName string, defaultTreatment string, changeNumber int64) {
}
