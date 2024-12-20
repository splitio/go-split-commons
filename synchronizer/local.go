package synchronizer

import (
	"time"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/flagsets"
	"github.com/splitio/go-split-commons/v6/healthcheck/application"
	"github.com/splitio/go-split-commons/v6/service/api"
	"github.com/splitio/go-split-commons/v6/storage"
	"github.com/splitio/go-split-commons/v6/synchronizer/worker/segment"
	"github.com/splitio/go-split-commons/v6/synchronizer/worker/split"
	"github.com/splitio/go-split-commons/v6/tasks"
	"github.com/splitio/go-toolkit/v5/logging"
)

// Local implements Local Synchronizer
type Local struct {
	splitTasks SplitTasks
	workers    Workers
	logger     logging.LoggerInterface
}

type LocalConfig struct {
	SplitPeriod      int
	SegmentPeriod    int
	SegmentWorkers   int
	QueueSize        int
	SegmentDirectory string
	RefreshEnabled   bool
	FlagSets         []string
}

// NewLocal creates new Local
func NewLocal(cfg *LocalConfig, splitAPI *api.SplitAPI, splitStorage storage.SplitStorage, segmentStorage storage.SegmentStorage, logger logging.LoggerInterface, runtimeTelemetry storage.TelemetryRuntimeProducer, hcMonitor application.MonitorProducerInterface) Synchronizer {
	workers := Workers{
		SplitUpdater: split.NewSplitUpdater(splitStorage, splitAPI.SplitFetcher, logger, runtimeTelemetry, hcMonitor, flagsets.NewFlagSetFilter(cfg.FlagSets)),
	}
	if cfg.SegmentDirectory != "" {
		workers.SegmentUpdater = segment.NewSegmentUpdater(splitStorage, segmentStorage, splitAPI.SegmentFetcher, logger, runtimeTelemetry, hcMonitor)
	}
	splitTasks := SplitTasks{}
	if cfg.RefreshEnabled {
		splitTasks.SplitSyncTask = tasks.NewFetchSplitsTask(workers.SplitUpdater, cfg.SplitPeriod, logger)
		if cfg.SegmentDirectory != "" {
			splitTasks.SegmentSyncTask = tasks.NewFetchSegmentsTask(workers.SegmentUpdater, cfg.SegmentPeriod, cfg.SegmentWorkers, cfg.QueueSize, logger, hcMonitor)
		}
	}

	return &Local{
		splitTasks: splitTasks,
		workers:    workers,
		logger:     logger,
	}
}

// SyncAll syncs splits and segments
func (s *Local) SyncAll() error {
	_, err := s.workers.SplitUpdater.SynchronizeSplits(nil)
	if err != nil {
		return err
	}
	if s.workers.SegmentUpdater != nil {
		_, err = s.workers.SegmentUpdater.SynchronizeSegments()
	}
	return err
}

// StartPeriodicFetching starts periodic fetchers tasks
func (s *Local) StartPeriodicFetching() {
	if s.splitTasks.SplitSyncTask != nil {
		s.splitTasks.SplitSyncTask.Start()
	}
	if s.splitTasks.SegmentSyncTask != nil {
		s.splitTasks.SegmentSyncTask.Start()
	}
}

// StopPeriodicFetching stops periodic fetchers tasks
func (s *Local) StopPeriodicFetching() {
	if s.splitTasks.SplitSyncTask != nil {
		s.splitTasks.SplitSyncTask.Stop(false)
	}
	if s.splitTasks.SegmentSyncTask != nil {
		s.splitTasks.SegmentSyncTask.Stop(true)
	}
}

// StartPeriodicDataRecording starts periodic recorders tasks
func (s *Local) StartPeriodicDataRecording() {
}

// StopPeriodicDataRecording stops periodic recorders tasks
func (s *Local) StopPeriodicDataRecording() {
}

// RefreshRates returns anything
func (s *Local) RefreshRates() (time.Duration, time.Duration) {
	return 10 * time.Minute, 10 * time.Minute
}

// SynchronizeSegment syncs segment
func (s *Local) SynchronizeSegment(name string, till *int64) error {
	if s.workers.SegmentUpdater != nil {
		_, err := s.workers.SegmentUpdater.SynchronizeSegment(name, till)
		return err
	}
	return nil
}

// SynchronizeLargeSegment syncs segment
func (s *Local) SynchronizeLargeSegment(name string, till *int64) error { return nil }

// SynchronizeLargeSegment syncs segment
func (s *Local) SynchronizeLargeSegmentUpdate(lsRFDResponseDTO *dtos.LargeSegmentRFDResponseDTO) error {
	return nil
}

// LocalKill local kill without any logic for localhost mode
func (s *Local) LocalKill(splitName string, defaultTreatment string, changeNumber int64) {}

// SynchronizeFeatureFlags no logic attached for localhost mode
func (s *Local) SynchronizeFeatureFlags(ffChange *dtos.SplitChangeUpdate) error { return nil }
