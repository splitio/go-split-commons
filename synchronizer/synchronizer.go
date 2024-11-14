package synchronizer

import (
	"time"

	"github.com/splitio/go-split-commons/v6/conf"
	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/synchronizer/worker/event"
	"github.com/splitio/go-split-commons/v6/synchronizer/worker/impression"
	"github.com/splitio/go-split-commons/v6/synchronizer/worker/impressionscount"
	"github.com/splitio/go-split-commons/v6/synchronizer/worker/largesegment"
	"github.com/splitio/go-split-commons/v6/synchronizer/worker/segment"
	"github.com/splitio/go-split-commons/v6/synchronizer/worker/split"
	"github.com/splitio/go-split-commons/v6/tasks"
	"github.com/splitio/go-split-commons/v6/telemetry"
	"github.com/splitio/go-toolkit/v5/asynctask"
	"github.com/splitio/go-toolkit/v5/logging"
)

// SplitTasks struct for tasks
type SplitTasks struct {
	SplitSyncTask            *asynctask.AsyncTask
	SegmentSyncTask          *asynctask.AsyncTask
	LargeSegmentSyncTask     *asynctask.AsyncTask
	TelemetrySyncTask        tasks.Task
	ImpressionSyncTask       tasks.Task
	EventSyncTask            tasks.Task
	ImpressionsCountSyncTask tasks.Task
	UniqueKeysTask           tasks.Task
	CleanFilterTask          tasks.Task
	ImpsCountConsumerTask    tasks.Task
}

// Workers struct for workers
type Workers struct {
	SplitUpdater             split.Updater
	SegmentUpdater           segment.Updater
	LargeSegmentUpdater      largesegment.Updater
	TelemetryRecorder        telemetry.TelemetrySynchronizer
	ImpressionRecorder       impression.ImpressionRecorder
	EventRecorder            event.EventRecorder
	ImpressionsCountRecorder impressionscount.ImpressionsCountRecorder
}

// Synchronizer interface for syncing data to and from splits servers
type Synchronizer interface {
	SyncAll() error
	SynchronizeFeatureFlags(ffChange *dtos.SplitChangeUpdate) error
	LocalKill(splitName string, defaultTreatment string, changeNumber int64)
	SynchronizeSegment(segmentName string, till *int64) error
	StartPeriodicFetching()
	StopPeriodicFetching()
	StartPeriodicDataRecording()
	StopPeriodicDataRecording()
	RefreshRates() (splits time.Duration, segments time.Duration)
	SynchronizeLargeSegment(name string, till *int64) error
}

// SynchronizerImpl implements Synchronizer
type SynchronizerImpl struct {
	splitTasks           SplitTasks
	workers              Workers
	logger               logging.LoggerInterface
	inMememoryFullQueue  chan string
	impressionBulkSize   int64
	eventBulkSize        int64
	splitsRefreshRate    int
	segmentsRefreshRate  int
	httpTiemoutSecs      int
	largeSegmentLazyLoad bool
}

// NewSynchronizer creates new SynchronizerImpl
func NewSynchronizer(
	confAdvanced conf.AdvancedConfig,
	splitTasks SplitTasks,
	workers Workers,
	logger logging.LoggerInterface,
	inMememoryFullQueue chan string,
) Synchronizer {
	return &SynchronizerImpl{
		impressionBulkSize:   confAdvanced.ImpressionsBulkSize,
		eventBulkSize:        confAdvanced.EventsBulkSize,
		splitTasks:           splitTasks,
		workers:              workers,
		logger:               logger,
		inMememoryFullQueue:  inMememoryFullQueue,
		splitsRefreshRate:    confAdvanced.SplitsRefreshRate,
		segmentsRefreshRate:  confAdvanced.SegmentsRefreshRate,
		httpTiemoutSecs:      confAdvanced.HTTPTimeout,
		largeSegmentLazyLoad: confAdvanced.LargeSegmentLazyLoad,
	}
}

// SyncAll syncs splits and segments
func (s *SynchronizerImpl) SyncAll() error {
	_, err := s.workers.SplitUpdater.SynchronizeSplits(nil)
	if err != nil {
		return err
	}
	_, err = s.workers.SegmentUpdater.SynchronizeSegments()
	if err != nil {
		return err
	}

	return s.synchronizeLargeSegments()
}

// StartPeriodicFetching starts periodic fetchers tasks
func (s *SynchronizerImpl) StartPeriodicFetching() {
	if s.splitTasks.SplitSyncTask != nil {
		s.splitTasks.SplitSyncTask.Start()
	}
	if s.splitTasks.SegmentSyncTask != nil {
		s.splitTasks.SegmentSyncTask.Start()
	}
	if s.splitTasks.LargeSegmentSyncTask != nil {
		s.splitTasks.LargeSegmentSyncTask.Start()
	}
}

// StopPeriodicFetching stops periodic fetchers tasks
func (s *SynchronizerImpl) StopPeriodicFetching() {
	if s.splitTasks.SplitSyncTask != nil {
		s.splitTasks.SplitSyncTask.Stop(false)
	}
	if s.splitTasks.SegmentSyncTask != nil {
		s.splitTasks.SegmentSyncTask.Stop(true)
	}
	if s.splitTasks.LargeSegmentSyncTask != nil {
		s.splitTasks.LargeSegmentSyncTask.Stop(true)
	}
}

// StartPeriodicDataRecording starts periodic recorders tasks
func (s *SynchronizerImpl) StartPeriodicDataRecording() {
	if s.inMememoryFullQueue != nil {
		go s.dataFlusher()
	}

	if s.splitTasks.ImpressionSyncTask != nil {
		s.splitTasks.ImpressionSyncTask.Start()
	}
	if s.splitTasks.TelemetrySyncTask != nil {
		s.splitTasks.TelemetrySyncTask.Start()
	}
	if s.splitTasks.EventSyncTask != nil {
		s.splitTasks.EventSyncTask.Start()
	}
	if s.splitTasks.ImpressionsCountSyncTask != nil {
		s.splitTasks.ImpressionsCountSyncTask.Start()
	}
	if s.splitTasks.UniqueKeysTask != nil {
		s.splitTasks.UniqueKeysTask.Start()
	}
	if s.splitTasks.CleanFilterTask != nil {
		s.splitTasks.CleanFilterTask.Start()
	}
	if s.splitTasks.ImpsCountConsumerTask != nil {
		s.splitTasks.ImpsCountConsumerTask.Start()
	}
}

// StopPeriodicDataRecording stops periodic recorders tasks
func (s *SynchronizerImpl) StopPeriodicDataRecording() {
	if s.splitTasks.ImpressionSyncTask != nil {
		s.splitTasks.ImpressionSyncTask.Stop(true)
	}
	if s.splitTasks.TelemetrySyncTask != nil {
		s.splitTasks.TelemetrySyncTask.Stop(true)
	}
	if s.splitTasks.EventSyncTask != nil {
		s.splitTasks.EventSyncTask.Stop(true)
	}
	if s.splitTasks.ImpressionsCountSyncTask != nil {
		s.splitTasks.ImpressionsCountSyncTask.Stop(true)
	}
	if s.splitTasks.UniqueKeysTask != nil {
		s.splitTasks.UniqueKeysTask.Stop(true)
	}
	if s.splitTasks.CleanFilterTask != nil {
		s.splitTasks.CleanFilterTask.Stop(true)
	}
	if s.splitTasks.ImpsCountConsumerTask != nil {
		s.splitTasks.ImpsCountConsumerTask.Stop(true)
	}
}

// RefreshRates returns the refresh rates of the splits & segment tasks
func (s *SynchronizerImpl) RefreshRates() (splits time.Duration, segments time.Duration) {
	return time.Duration(s.splitsRefreshRate) * time.Second, time.Duration(s.segmentsRefreshRate) * time.Second
}

// LocalKill locally kills a split
func (s *SynchronizerImpl) LocalKill(splitName string, defaultTreatment string, changeNumber int64) {
	s.workers.SplitUpdater.LocalKill(splitName, defaultTreatment, changeNumber)
}

// SynchronizeSegment syncs segment
func (s *SynchronizerImpl) SynchronizeSegment(name string, till *int64) error {
	_, err := s.workers.SegmentUpdater.SynchronizeSegment(name, till)
	return err
}

// SynchronizeSegment syncs segment
func (s *SynchronizerImpl) SynchronizeLargeSegment(name string, till *int64) error {
	return s.workers.LargeSegmentUpdater.SynchronizeLargeSegment(name, till)
}

// SynchronizeFeatureFlags syncs featureFlags
func (s *SynchronizerImpl) SynchronizeFeatureFlags(ffChange *dtos.SplitChangeUpdate) error {
	result, err := s.workers.SplitUpdater.SynchronizeFeatureFlags(ffChange)
	s.synchronizeSegmentsAfterSplitSync(result.ReferencedSegments, result.ReferencedLargeSegments)
	return err
}

var _ Synchronizer = &SynchronizerImpl{}
