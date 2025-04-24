package tasks

import (
	"github.com/splitio/go-split-commons/v7/telemetry"
	"github.com/splitio/go-toolkit/v5/asynctask"
	"github.com/splitio/go-toolkit/v5/logging"
)

// NewRecordUniqueKeysTask constructor
func NewRecordUniqueKeysTask(
	recorder telemetry.TelemetrySynchronizer,
	period int,
	logger logging.LoggerInterface,
	bulkSize int64,
) *asynctask.AsyncTask {
	record := func(logger logging.LoggerInterface) error {
		return recorder.SynchronizeUniqueKeys(bulkSize)
	}

	onStop := func(logger logging.LoggerInterface) {
		recorder.SynchronizeUniqueKeys(bulkSize)
	}

	return asynctask.NewAsyncTask("SubmitUniqueKeys", record, period, nil, onStop, logger)
}
