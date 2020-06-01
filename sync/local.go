package sync

import (
	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service"
	"github.com/splitio/go-split-commons/storage"
	storageMock "github.com/splitio/go-split-commons/storage/mocks"
	"github.com/splitio/go-split-commons/synchronizer"
	"github.com/splitio/go-split-commons/tasks"
	"github.com/splitio/go-toolkit/logging"
)

// LocalSynchronizer implements Synchronizer
type LocalSynchronizer struct {
	splitTasks          splitTasks
	synchronizers       synchronizers
	logger              logging.LoggerInterface
	inMememoryFullQueue chan string
}

// NewLocalSynchronizer creates new SynchronizerImpl
func NewLocalSynchronizer(
	period int,
	splitAPI *service.SplitAPI,
	splitStorage storage.SplitStorage,
	logger logging.LoggerInterface,
) Synchronizer {
	metricStorageMock := storageMock.MockMetricStorage{
		IncCounterCall:   func(key string) {},
		IncLatencyCall:   func(metricName string, index int) {},
		PopCountersCall:  func() []dtos.CounterDTO { return make([]dtos.CounterDTO, 0, 0) },
		PopGaugesCall:    func() []dtos.GaugeDTO { return make([]dtos.GaugeDTO, 0, 0) },
		PopLatenciesCall: func() []dtos.LatenciesDTO { return make([]dtos.LatenciesDTO, 0, 0) },
		PutGaugeCall:     func(key string, gauge float64) {},
	}
	synchronizers := synchronizers{
		splitSynchronizer: synchronizer.NewSplitSynchronizer(splitStorage, splitAPI.SplitFetcher, metricStorageMock, logger),
	}
	return &LocalSynchronizer{
		splitTasks: splitTasks{
			splitSyncTask: tasks.NewFetchSplitsTask(synchronizers.splitSynchronizer, period, logger),
		},
		synchronizers: synchronizers,
		logger:        logger,
	}
}

// SyncAll syncs splits and segments
func (s *LocalSynchronizer) SyncAll() error {
	return s.synchronizers.splitSynchronizer.SynchronizeSplits(nil)
}

// StartPeriodicFetching starts periodic fetchers tasks
func (s *LocalSynchronizer) StartPeriodicFetching() {
	s.splitTasks.splitSyncTask.Start()
}

// StopPeriodicFetching stops periodic fetchers tasks
func (s *LocalSynchronizer) StopPeriodicFetching() {
	s.splitTasks.splitSyncTask.Stop(false)
}

// StartPeriodicDataRecording starts periodic recorders tasks
func (s *LocalSynchronizer) StartPeriodicDataRecording() {
}

// StopPeriodicDataRecording stops periodic recorders tasks
func (s *LocalSynchronizer) StopPeriodicDataRecording() {
}

// SynchronizeSplits syncs splits
func (s *LocalSynchronizer) SynchronizeSplits(till *int64) error {
	return s.synchronizers.splitSynchronizer.SynchronizeSplits(till)
}

// SynchronizeSegment syncs segment
func (s *LocalSynchronizer) SynchronizeSegment(name string, till *int64) error {
	return nil
}
