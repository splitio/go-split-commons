package tasks

import (
	"github.com/splitio/go-split-commons/synchronizer/worker/impression"
	"github.com/splitio/go-toolkit/asynctask"
	"github.com/splitio/go-toolkit/logging"
)

// NewRecordImpressionsTask creates a new splits fetching and storing task
func NewRecordImpressionsTask(
	recorder impression.ImpressionRecorder,
	period int,
	logger logging.LoggerInterface,
	bulkSize int64,
) *asynctask.AsyncTask {
	record := func(logger logging.LoggerInterface) error {
		return recorder.SynchronizeImpressions(bulkSize)
	}

	onStop := func(logger logging.LoggerInterface) {
		// All this function does is flush impressions which will clear the storage
		recorder.FlushImpressions(bulkSize)
	}

	return asynctask.NewAsyncTask("SubmitImpressions", record, period, nil, onStop, logger)
}
