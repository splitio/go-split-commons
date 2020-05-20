package tasks

import (
	"github.com/splitio/go-split-commons/synchronizer"
	"github.com/splitio/go-toolkit/asynctask"
	"github.com/splitio/go-toolkit/logging"
)

/*
func onStopAction(
	eventStorage storage.EventStorageConsumer,
	eventRecorder service.EventsRecorder,
	bulkSize int64,
	logger logging.LoggerInterface,
) {

	for !eventStorage.Empty() {
		submitEvents(
			eventStorage,
			eventRecorder,
			bulkSize,
			logger,
		)
	}

}
*/

// NewRecordEventsTask creates a new events recording task
func NewRecordEventsTask(
	synchronizer *synchronizer.EventSynchronizer,
	bulkSize int64,
	period int,
	logger logging.LoggerInterface,
) *asynctask.AsyncTask {
	record := func(logger logging.LoggerInterface) error {
		return synchronizer.SynchronizeEvents(bulkSize)
	}

	onStop := func(logger logging.LoggerInterface) {
		// All this function does is flush events which will clear the storage
		record(logger)
		// onStopAction(synchronizer, bulkSize, logger)
	}

	return asynctask.NewAsyncTask("SubmitEvents", record, period, nil, onStop, logger)
}
