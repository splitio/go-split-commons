package tasks

import (
	"github.com/splitio/go-split-commons/v6/telemetry"
	"github.com/splitio/go-toolkit/v5/asynctask"
	"github.com/splitio/go-toolkit/v5/logging"
)

// NewRecordUniqueKeysTask constructor
func NewRecordUniqueKeysTask(
	recorder telemetry.TelemetrySynchronizer,
	period int,
	logger logging.LoggerInterface,
) *asynctask.AsyncTask {
	record := func(logger logging.LoggerInterface) error {
		return recorder.SynchronizeUniqueKeys()
	}

	onStop := func(logger logging.LoggerInterface) {
		recorder.SynchronizeUniqueKeys()
	}

	return asynctask.NewAsyncTask("SubmitUniqueKeys", record, period, nil, onStop, logger)
}
