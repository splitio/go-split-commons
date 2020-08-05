package synchronizer

import (
	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-split-commons/synchronizer/worker/event"
	"github.com/splitio/go-split-commons/synchronizer/worker/impression"
	"github.com/splitio/go-split-commons/synchronizer/worker/metric"
	"github.com/splitio/go-split-commons/synchronizer/worker/segment"
	"github.com/splitio/go-split-commons/synchronizer/worker/split"
	"github.com/splitio/go-split-commons/tasks"
	"github.com/splitio/go-toolkit/asynctask"
	"github.com/splitio/go-toolkit/logging"
)

// splitTasks struct for tasks
type splitTasks struct {
	splitSyncTask      *asynctask.AsyncTask
	segmentSyncTask    *asynctask.AsyncTask
	telemetrySyncTask  *asynctask.AsyncTask
	impressionSyncTask *asynctask.AsyncTask
	eventSyncTask      *asynctask.AsyncTask
}

// Workers struct for workers
type Workers struct {
	SplitFetcher       split.SplitFetcher
	SegmentFetcher     segment.SegmentFetcher
	TelemetryRecorder  metric.MetricRecorder
	ImpressionRecorder impression.ImpressionRecorder
	EventRecorder      event.EventRecorder
}

// SynchronizerImpl implements Synchronizer
type SynchronizerImpl struct {
	splitTasks          splitTasks
	workers             Workers
	logger              logging.LoggerInterface
	inMememoryFullQueue chan string
	impressionBulkSize  int64
	eventBulkSize       int64
}

func setupTasks(
	workers Workers,
	confTask conf.TaskPeriods,
	confAdvanced conf.AdvancedConfig,
	logger logging.LoggerInterface,
) splitTasks {
	splitTasks := splitTasks{}
	if workers.SplitFetcher != nil {
		splitTasks.splitSyncTask = tasks.NewFetchSplitsTask(workers.SplitFetcher, confTask.SplitSync, logger)
	}
	if workers.SegmentFetcher != nil {
		splitTasks.segmentSyncTask = tasks.NewFetchSegmentsTask(workers.SegmentFetcher, confTask.SegmentSync, confAdvanced.SegmentWorkers, confAdvanced.SegmentQueueSize, logger)
	}
	if workers.TelemetryRecorder != nil {
		splitTasks.telemetrySyncTask = tasks.NewRecordTelemetryTask(workers.TelemetryRecorder, confTask.CounterSync, logger)
	}
	if workers.ImpressionRecorder != nil {
		splitTasks.impressionSyncTask = tasks.NewRecordImpressionsTask(workers.ImpressionRecorder, confTask.ImpressionSync, logger, confAdvanced.ImpressionsBulkSize)
	}
	if workers.EventRecorder != nil {
		splitTasks.eventSyncTask = tasks.NewRecordEventsTask(workers.EventRecorder, confAdvanced.EventsBulkSize, confTask.EventsSync, logger)
	}
	return splitTasks
}

// NewSynchronizer creates new SynchronizerImpl
func NewSynchronizer(
	confTask conf.TaskPeriods,
	confAdvanced conf.AdvancedConfig,
	workers Workers,
	logger logging.LoggerInterface,
	inMememoryFullQueue chan string,
) Synchronizer {
	return &SynchronizerImpl{
		impressionBulkSize:  confAdvanced.ImpressionsBulkSize,
		eventBulkSize:       confAdvanced.EventsBulkSize,
		splitTasks:          setupTasks(workers, confTask, confAdvanced, logger),
		workers:             workers,
		logger:              logger,
		inMememoryFullQueue: inMememoryFullQueue,
	}
}

func (s *SynchronizerImpl) dataFlusher() {
	for true {
		msg := <-s.inMememoryFullQueue
		switch msg {
		case "EVENTS_FULL":
			s.logger.Debug("FLUSHING storage queue")
			err := s.workers.EventRecorder.SynchronizeEvents(s.eventBulkSize)
			if err != nil {
				s.logger.Error("Error flushing storage queue", err)
			}
			break
		case "IMPRESSIONS_FULL":
			s.logger.Debug("FLUSHING storage queue")
			err := s.workers.ImpressionRecorder.SynchronizeImpressions(s.impressionBulkSize)
			if err != nil {
				s.logger.Error("Error flushing storage queue", err)
			}
		}
	}
}

// SyncAll syncs splits and segments
func (s *SynchronizerImpl) SyncAll() error {
	err := s.workers.SplitFetcher.SynchronizeSplits(nil)
	if err != nil {
		return err
	}
	return s.workers.SegmentFetcher.SynchronizeSegments()
}

// StartPeriodicFetching starts periodic fetchers tasks
func (s *SynchronizerImpl) StartPeriodicFetching() {
	if s.splitTasks.splitSyncTask != nil {
		s.splitTasks.splitSyncTask.Start()
	}
	if s.splitTasks.segmentSyncTask != nil {
		s.splitTasks.segmentSyncTask.Start()
	}
}

// StopPeriodicFetching stops periodic fetchers tasks
func (s *SynchronizerImpl) StopPeriodicFetching() {
	if s.splitTasks.splitSyncTask != nil {
		s.splitTasks.splitSyncTask.Stop(false)
	}
	if s.splitTasks.segmentSyncTask != nil {
		s.splitTasks.segmentSyncTask.Stop(true)
	}
}

// StartPeriodicDataRecording starts periodic recorders tasks
func (s *SynchronizerImpl) StartPeriodicDataRecording() {
	if s.inMememoryFullQueue != nil {
		go s.dataFlusher()
	}

	if s.splitTasks.impressionSyncTask != nil {
		s.splitTasks.impressionSyncTask.Start()
	}
	if s.splitTasks.telemetrySyncTask != nil {
		s.splitTasks.telemetrySyncTask.Start()
	}
	if s.splitTasks.eventSyncTask != nil {
		s.splitTasks.eventSyncTask.Start()
	}
}

// StopPeriodicDataRecording stops periodic recorders tasks
func (s *SynchronizerImpl) StopPeriodicDataRecording() {
	if s.splitTasks.impressionSyncTask != nil {
		s.splitTasks.impressionSyncTask.Stop(true)
	}
	if s.splitTasks.telemetrySyncTask != nil {
		s.splitTasks.telemetrySyncTask.Stop(false)
	}
	if s.splitTasks.eventSyncTask != nil {
		s.splitTasks.eventSyncTask.Stop(true)
	}
}

// SynchronizeSplits syncs splits
func (s *SynchronizerImpl) SynchronizeSplits(till *int64) error {
	return s.workers.SplitFetcher.SynchronizeSplits(till)
}

// SynchronizeSegment syncs segment
func (s *SynchronizerImpl) SynchronizeSegment(name string, till *int64) error {
	return s.workers.SegmentFetcher.SynchronizeSegment(name, till)
}
