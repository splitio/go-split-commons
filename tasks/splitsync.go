package tasks

import (
	"github.com/splitio/go-split-commons/synchronizer"
	"github.com/splitio/go-toolkit/asynctask"
	"github.com/splitio/go-toolkit/logging"
)

// NewFetchSplitsTask creates a new splits fetching and storing task
func NewFetchSplitsTask(
	synchronizer *synchronizer.SplitSynchronizer,
	period int,
	logger logging.LoggerInterface,
) *asynctask.AsyncTask {
	update := func(logger logging.LoggerInterface) error {
		return synchronizer.SynchronizeSplits(nil)
	}

	return asynctask.NewAsyncTask("UpdateSplits", update, period, nil, nil, logger)
}
