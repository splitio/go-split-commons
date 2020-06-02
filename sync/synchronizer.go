package sync

import (
	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service"
	"github.com/splitio/go-split-commons/storage"
	"github.com/splitio/go-split-commons/synchronizer"
	"github.com/splitio/go-split-commons/synchronizer/event"
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

// synchronizers struct for synchronizers
type synchronizers struct {
	splitSynchronizer      *synchronizer.SplitSynchronizer
	segmentSynchronizer    *synchronizer.SegmentSynchronizer
	metricSynchronizer     *synchronizer.MetricSynchronizer
	impressionSynchronizer *synchronizer.ImpressionSynchronizer
	eventSynchronizer      event.EventSynchronizer
}

// SynchronizerImpl implements Synchronizer
type SynchronizerImpl struct {
	splitTasks          splitTasks
	synchronizers       synchronizers
	logger              logging.LoggerInterface
	inMememoryFullQueue chan string
	impressionBulkSize  int64
	eventBulkSize       int64
}

func setupSynchronizers(
	splitAPI *service.SplitAPI,
	splitStorage storage.SplitStorage,
	segmentStorage storage.SegmentStorage,
	metricStorage storage.MetricsStorage,
	impressionStorage storage.ImpressionStorage,
	eventStorage storage.EventsStorage,
	logger logging.LoggerInterface,
	metadata *dtos.Metadata,
) synchronizers {
	if metadata != nil {
		return synchronizers{
			splitSynchronizer:      synchronizer.NewSplitSynchronizer(splitStorage, splitAPI.SplitFetcher, metricStorage, logger),
			segmentSynchronizer:    synchronizer.NewSegmentSynchronizer(splitStorage, segmentStorage, splitAPI.SegmentFetcher, metricStorage, logger),
			metricSynchronizer:     synchronizer.NewMetricSynchronizer(metricStorage, splitAPI.MetricRecorder, *metadata),
			impressionSynchronizer: synchronizer.NewImpressionSynchronizer(impressionStorage, splitAPI.ImpressionRecorder, metricStorage, logger, *metadata),
			eventSynchronizer:      event.NewEventSynchronizerSingle(eventStorage, splitAPI.EventRecorder, metricStorage, logger, *metadata),
		}
	}
	return synchronizers{
		splitSynchronizer:   synchronizer.NewSplitSynchronizer(splitStorage, splitAPI.SplitFetcher, metricStorage, logger),
		segmentSynchronizer: synchronizer.NewSegmentSynchronizer(splitStorage, segmentStorage, splitAPI.SegmentFetcher, metricStorage, logger),
		// metricSynchronizer:     synchronizer.NewMetricSynchronizer(metricStorage, splitAPI.MetricRecorder),
		// impressionSynchronizer: synchronizer.NewImpressionSynchronizer(impressionStorage, splitAPI.ImpressionRecorder, metricStorage, logger),
		eventSynchronizer: event.NewEventSynchronizerMultiple(eventStorage, splitAPI.EventRecorder, metricStorage, logger),
	}
}

func setupTasks(
	synchronizers synchronizers,
	confTask conf.TaskPeriods,
	confAdvanced conf.AdvancedConfig,
	logger logging.LoggerInterface,
) splitTasks {
	return splitTasks{
		splitSyncTask:      tasks.NewFetchSplitsTask(synchronizers.splitSynchronizer, confTask.SplitSync, logger),
		segmentSyncTask:    tasks.NewFetchSegmentsTask(synchronizers.segmentSynchronizer, confTask.SegmentSync, confAdvanced.SegmentWorkers, confAdvanced.SegmentQueueSize, logger),
		telemetrySyncTask:  tasks.NewRecordTelemetryTask(synchronizers.metricSynchronizer, confTask.CounterSync, logger),
		impressionSyncTask: tasks.NewRecordImpressionsTask(synchronizers.impressionSynchronizer, confTask.ImpressionSync, logger, confAdvanced.ImpressionsBulkSize),
		eventSyncTask:      tasks.NewRecordEventsTask(synchronizers.eventSynchronizer, confAdvanced.EventsBulkSize, confTask.EventsSync, logger),
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
	splitSynchronizers := setupSynchronizers(
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
		splitTasks:          setupTasks(splitSynchronizers, confTask, confAdvanced, logger),
		synchronizers:       splitSynchronizers,
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
			err := s.synchronizers.eventSynchronizer.SynchronizeEvents(s.eventBulkSize)
			if err != nil {
				s.logger.Error("Error flushing storage queue", err)
			}
			break
		case "IMPRESSIONS_FULL":
			s.logger.Debug("FLUSHING storage queue")
			err := s.synchronizers.impressionSynchronizer.SynchronizeImpressions(s.impressionBulkSize)
			if err != nil {
				s.logger.Error("Error flushing storage queue", err)
			}
		}
	}
}

// SyncAll syncs splits and segments
func (s *SynchronizerImpl) SyncAll() error {
	err := s.synchronizers.splitSynchronizer.SynchronizeSplits(nil)
	if err != nil {
		return err
	}
	return s.synchronizers.segmentSynchronizer.SynchronizeSegments()
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
	return s.synchronizers.splitSynchronizer.SynchronizeSplits(till)
}

// SynchronizeSegment syncs segment
func (s *SynchronizerImpl) SynchronizeSegment(name string, till *int64) error {
	return s.synchronizers.segmentSynchronizer.SynchronizeSegment(name, till)
}
