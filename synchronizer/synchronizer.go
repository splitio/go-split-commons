package synchronizer

import (
	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service"
	"github.com/splitio/go-split-commons/storage"
	"github.com/splitio/go-split-commons/synchronizer/worker"
	"github.com/splitio/go-split-commons/synchronizer/worker/event"
	"github.com/splitio/go-split-commons/synchronizer/worker/impression"
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

// workers struct for workers
type workers struct {
	splitFetcher       *worker.SplitFetcher
	segmentFetcher     *worker.SegmentFetcher
	telemetryRecorder  *worker.MetricRecorder
	impressionRecorder impression.ImpressionRecorder
	eventRecorder      event.EventRecorder
}

// SynchronizerImpl implements Synchronizer
type SynchronizerImpl struct {
	splitTasks          splitTasks
	workers             workers
	logger              logging.LoggerInterface
	inMememoryFullQueue chan string
	impressionBulkSize  int64
	eventBulkSize       int64
}

func setupWorkers(
	splitAPI *service.SplitAPI,
	splitStorage storage.SplitStorage,
	segmentStorage storage.SegmentStorage,
	metricStorage storage.MetricsStorage,
	impressionStorage storage.ImpressionStorage,
	eventStorage storage.EventsStorage,
	logger logging.LoggerInterface,
	metadata *dtos.Metadata,
) workers {
	return workers{
		splitFetcher:       worker.NewSplitFetcher(splitStorage, splitAPI.SplitFetcher, metricStorage, logger),
		segmentFetcher:     worker.NewSegmentFetcher(splitStorage, segmentStorage, splitAPI.SegmentFetcher, metricStorage, logger),
		telemetryRecorder:  worker.NewMetricRecorder(metricStorage, splitAPI.MetricRecorder, *metadata),
		impressionRecorder: impression.NewRecorderSingle(impressionStorage, splitAPI.ImpressionRecorder, metricStorage, logger, *metadata),
		eventRecorder:      event.NewEventRecorderSingle(eventStorage, splitAPI.EventRecorder, metricStorage, logger, *metadata),
	}
}

func setupTasks(
	workers workers,
	confTask conf.TaskPeriods,
	confAdvanced conf.AdvancedConfig,
	logger logging.LoggerInterface,
) splitTasks {
	return splitTasks{
		splitSyncTask:      tasks.NewFetchSplitsTask(workers.splitFetcher, confTask.SplitSync, logger),
		segmentSyncTask:    tasks.NewFetchSegmentsTask(workers.segmentFetcher, confTask.SegmentSync, confAdvanced.SegmentWorkers, confAdvanced.SegmentQueueSize, logger),
		telemetrySyncTask:  tasks.NewRecordTelemetryTask(workers.telemetryRecorder, confTask.CounterSync, logger),
		impressionSyncTask: tasks.NewRecordImpressionsTask(workers.impressionRecorder, confTask.ImpressionSync, logger, confAdvanced.ImpressionsBulkSize),
		eventSyncTask:      tasks.NewRecordEventsTask(workers.eventRecorder, confAdvanced.EventsBulkSize, confTask.EventsSync, logger),
	}
}

// NewSynchronizer creates new SynchronizerImpl
func NewSynchronizer(
	confTask conf.TaskPeriods,
	confAdvanced conf.AdvancedConfig,
	splitAPI *service.SplitAPI,
	splitStorage storage.SplitStorage,
	segmentStorage storage.SegmentStorage,
	metricStorage storage.MetricsStorage,
	impressionStorage storage.ImpressionStorage,
	eventStorage storage.EventsStorage,
	logger logging.LoggerInterface,
	inMememoryFullQueue chan string,
	metadata *dtos.Metadata,
) Synchronizer {
	splitWorkers := setupWorkers(
		splitAPI,
		splitStorage,
		segmentStorage,
		metricStorage,
		impressionStorage,
		eventStorage,
		logger,
		metadata,
	)
	return &SynchronizerImpl{
		impressionBulkSize:  confAdvanced.ImpressionsBulkSize,
		eventBulkSize:       confAdvanced.EventsBulkSize,
		splitTasks:          setupTasks(splitWorkers, confTask, confAdvanced, logger),
		workers:             splitWorkers,
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
			err := s.workers.eventRecorder.SynchronizeEvents(s.eventBulkSize)
			if err != nil {
				s.logger.Error("Error flushing storage queue", err)
			}
			break
		case "IMPRESSIONS_FULL":
			s.logger.Debug("FLUSHING storage queue")
			err := s.workers.impressionRecorder.SynchronizeImpressions(s.impressionBulkSize)
			if err != nil {
				s.logger.Error("Error flushing storage queue", err)
			}
		}
	}
}

// SyncAll syncs splits and segments
func (s *SynchronizerImpl) SyncAll() error {
	err := s.workers.splitFetcher.SynchronizeSplits(nil)
	if err != nil {
		return err
	}
	return s.workers.segmentFetcher.SynchronizeSegments()
}

// StartPeriodicFetching starts periodic fetchers tasks
func (s *SynchronizerImpl) StartPeriodicFetching() {
	s.splitTasks.splitSyncTask.Start()
	s.splitTasks.segmentSyncTask.Start()
}

// StopPeriodicFetching stops periodic fetchers tasks
func (s *SynchronizerImpl) StopPeriodicFetching() {
	s.splitTasks.splitSyncTask.Stop(false)
	s.splitTasks.segmentSyncTask.Stop(false)
}

// StartPeriodicDataRecording starts periodic recorders tasks
func (s *SynchronizerImpl) StartPeriodicDataRecording() {
	if s.inMememoryFullQueue != nil {
		go s.dataFlusher()
	}

	s.splitTasks.impressionSyncTask.Start()
	s.splitTasks.telemetrySyncTask.Start()
	s.splitTasks.eventSyncTask.Start()
}

// StopPeriodicDataRecording stops periodic recorders tasks
func (s *SynchronizerImpl) StopPeriodicDataRecording() {
	s.splitTasks.impressionSyncTask.Stop(true)
	s.splitTasks.telemetrySyncTask.Stop(false)
	s.splitTasks.eventSyncTask.Stop(true)
}

// SynchronizeSplits syncs splits
func (s *SynchronizerImpl) SynchronizeSplits(till *int64) error {
	return s.workers.splitFetcher.SynchronizeSplits(till)
}

// SynchronizeSegment syncs segment
func (s *SynchronizerImpl) SynchronizeSegment(name string, till *int64) error {
	return s.workers.segmentFetcher.SynchronizeSegment(name, till)
}
