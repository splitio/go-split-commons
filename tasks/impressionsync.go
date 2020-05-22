package tasks

import (
	"github.com/splitio/go-split-commons/synchronizer"
	"github.com/splitio/go-toolkit/asynctask"
	"github.com/splitio/go-toolkit/logging"
)

// NewRecordImpressionsTask creates a new splits fetching and storing task
func NewRecordImpressionsTask(
	synchronizer *synchronizer.ImpressionSynchronizer,
	period int,
	logger logging.LoggerInterface,
	bulkSize int64,
) *asynctask.AsyncTask {
	record := func(logger logging.LoggerInterface) error {
		return synchronizer.SynchronizeImpressions(bulkSize)
	}

	onStop := func(logger logging.LoggerInterface) {
		// All this function does is flush impressions which will clear the storage
		synchronizer.FlushImpressions(bulkSize)
	}

	return asynctask.NewAsyncTask("SubmitImpressions", record, period, nil, onStop, logger)
}
