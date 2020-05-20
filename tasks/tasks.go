package tasks

import (
	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-split-commons/synchronizer"
	"github.com/splitio/go-toolkit/asynctask"
	"github.com/splitio/go-toolkit/logging"
)

// SplitTasks struct for tasks
type SplitTasks struct {
	SplitTask          *asynctask.AsyncTask
	SegmentSyncTask    *asynctask.AsyncTask
	CounterSyncTask    *asynctask.AsyncTask
	LatencySyncTask    *asynctask.AsyncTask
	GaugeSyncTask      *asynctask.AsyncTask
	ImpressionSyncTask *asynctask.AsyncTask
	EventSyncTask      *asynctask.AsyncTask
}

// NewSplitTasks creates new SplitTasks
func NewSplitTasks(
	synchronizers synchronizer.SplitSynchronizers,
	confTask conf.TaskPeriods,
	confAdvanced conf.AdvancedConfig,
	logger logging.LoggerInterface,
) *SplitTasks {
	return &SplitTasks{
		SplitTask:          NewFetchSplitsTask(synchronizers.SplitSynchronizer, confTask.SplitSync, logger),
		SegmentSyncTask:    NewFetchSegmentsTask(synchronizers.SegmentSynchronizer, confTask.SegmentSync, confAdvanced.SegmentWorkers, confAdvanced.SegmentQueueSize, logger),
		CounterSyncTask:    NewRecordCountersTask(synchronizers.MetricSynchronizer, confTask.CounterSync, logger),
		LatencySyncTask:    NewRecordCountersTask(synchronizers.MetricSynchronizer, confTask.CounterSync, logger),
		GaugeSyncTask:      NewRecordCountersTask(synchronizers.MetricSynchronizer, confTask.CounterSync, logger),
		ImpressionSyncTask: NewRecordImpressionsTask(synchronizers.ImpressionSynchronizer, confTask.ImpressionSync, logger, confAdvanced.ImpressionsBulkSize),
		EventSyncTask:      NewRecordEventsTask(synchronizers.EventSynchronizer, confAdvanced.EventsBulkSize, confTask.EventsSync, logger),
	}
}
