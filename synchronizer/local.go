package synchronizer

import (
	"time"

	"github.com/splitio/go-split-commons/v4/healthcheck/application"
	"github.com/splitio/go-split-commons/v4/service/api"
	"github.com/splitio/go-split-commons/v4/storage"
	"github.com/splitio/go-split-commons/v4/synchronizer/worker/segment"
	"github.com/splitio/go-split-commons/v4/synchronizer/worker/split"
	"github.com/splitio/go-split-commons/v4/tasks"
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
}

// NewLocal creates new Local
func NewLocal(cfg *LocalConfig, splitAPI *api.SplitAPI, splitStorage storage.SplitStorage, segmentStorage storage.SegmentStorage, logger logging.LoggerInterface, runtimeTelemetry storage.TelemetryRuntimeProducer, hcMonitor application.MonitorProducerInterface) Synchronizer {
	workers := Workers{
		SplitFetcher: split.NewSplitFetcher(splitStorage, splitAPI.SplitFetcher, logger, runtimeTelemetry, hcMonitor),
	}
	if cfg.SegmentDirectory != "" {
		workers.SegmentFetcher = segment.NewSegmentFetcher(splitStorage, segmentStorage, splitAPI.SegmentFetcher, logger, runtimeTelemetry, hcMonitor)
	}
	var splitTasks SplitTasks
	if cfg.RefreshEnabled {
		splitTasks = SplitTasks{
			SplitSyncTask: tasks.NewFetchSplitsTask(workers.SplitFetcher, cfg.SplitPeriod, logger),
		}
		if cfg.SegmentDirectory != "" {
			splitTasks.SegmentSyncTask = tasks.NewFetchSegmentsTask(workers.SegmentFetcher, cfg.SegmentPeriod, cfg.SegmentWorkers, cfg.QueueSize, logger)
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
	_, err := s.workers.SplitFetcher.SynchronizeSplits(nil)
	if err != nil {
		return err
	}
	if s.workers.SegmentFetcher != nil {
		_, err = s.workers.SegmentFetcher.SynchronizeSegments()
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

// SynchronizeSplits syncs splits
func (s *Local) SynchronizeSplits(till *int64) error {
	result, err := s.workers.SplitFetcher.SynchronizeSplits(till)
	if s.workers.SegmentFetcher != nil {
		for _, segment := range s.filterCachedLocalSegments(result.ReferencedSegments) {
			go s.SynchronizeSegment(segment, nil) // send segment to workerpool (queue is bypassed)
		}
	}
	return err
}

func (s *Local) filterCachedLocalSegments(segmentsReferenced []string) []string {
	toRet := make([]string, 0, len(segmentsReferenced))
	for _, name := range segmentsReferenced {
		if !s.workers.SegmentFetcher.IsSegmentCached(name) {
			toRet = append(toRet, name)
		}
	}
	return toRet
}

// SynchronizeSegment syncs segment
func (s *Local) SynchronizeSegment(name string, till *int64) error {
	if s.workers.SegmentFetcher != nil {
		_, err := s.workers.SegmentFetcher.SynchronizeSegment(name, till)
		return err
	}
	return nil
}

// LocalKill does nothing
func (s *Local) LocalKill(splitName string, defaultTreatment string, changeNumber int64) {
}
