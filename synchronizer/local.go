package synchronizer

import (
	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service"
	"github.com/splitio/go-split-commons/storage"
	storageMock "github.com/splitio/go-split-commons/storage/mocks"
	"github.com/splitio/go-split-commons/synchronizer/worker"
	"github.com/splitio/go-split-commons/tasks"
	"github.com/splitio/go-toolkit/logging"
)

// Local implements Local Synchronizer
type Local struct {
	splitTasks          splitTasks
	workers             workers
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
	metricStorageMock := storageMock.MockMetricStorage{
		IncCounterCall:   func(key string) {},
		IncLatencyCall:   func(metricName string, index int) {},
		PopCountersCall:  func() []dtos.CounterDTO { return make([]dtos.CounterDTO, 0, 0) },
		PopGaugesCall:    func() []dtos.GaugeDTO { return make([]dtos.GaugeDTO, 0, 0) },
		PopLatenciesCall: func() []dtos.LatenciesDTO { return make([]dtos.LatenciesDTO, 0, 0) },
		PutGaugeCall:     func(key string, gauge float64) {},
	}
	workers := workers{
		splitFetcher: worker.NewSplitFetcher(splitStorage, splitAPI.SplitFetcher, metricStorageMock, logger),
	}
	return &Local{
		splitTasks: splitTasks{
			splitSyncTask: tasks.NewFetchSplitsTask(workers.splitFetcher, period, logger),
		},
		workers: workers,
		logger:  logger,
	}
}

// SyncAll syncs splits and segments
func (s *Local) SyncAll() error {
	return s.workers.splitFetcher.SynchronizeSplits(nil)
}

// StartPeriodicFetching starts periodic fetchers tasks
func (s *Local) StartPeriodicFetching() {
	s.splitTasks.splitSyncTask.Start()
}

// StopPeriodicFetching stops periodic fetchers tasks
func (s *Local) StopPeriodicFetching() {
	s.splitTasks.splitSyncTask.Stop(false)
}

// StartPeriodicDataRecording starts periodic recorders tasks
func (s *Local) StartPeriodicDataRecording() {
}

// StopPeriodicDataRecording stops periodic recorders tasks
func (s *Local) StopPeriodicDataRecording() {
}

// SynchronizeSplits syncs splits
func (s *Local) SynchronizeSplits(till *int64) error {
	return s.workers.splitFetcher.SynchronizeSplits(till)
}

// SynchronizeSegment syncs segment
func (s *Local) SynchronizeSegment(name string, till *int64) error {
	return nil
}
