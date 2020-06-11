package tasks

import (
	"github.com/splitio/go-split-commons/synchronizer/worker/event"
	"github.com/splitio/go-toolkit/asynctask"
	"github.com/splitio/go-toolkit/logging"
)

// NewRecordEventsTask creates a new events recording task
func NewRecordEventsTask(
	synchronizer event.EventRecorder,
	bulkSize int64,
	period int,
	logger logging.LoggerInterface,
) *asynctask.AsyncTask {
	record := func(logger logging.LoggerInterface) error {
		return synchronizer.SynchronizeEvents(bulkSize)
	}

	onStop := func(logger logging.LoggerInterface) {
		// All this function does is flush events which will clear the storage
		synchronizer.FlushEvents(bulkSize)
	}

	return asynctask.NewAsyncTask("SubmitEvents", record, period, nil, onStop, logger)
}
