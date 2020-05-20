package tasks

import (
	"github.com/splitio/go-split-commons/synchronizer"
	"github.com/splitio/go-toolkit/asynctask"
	"github.com/splitio/go-toolkit/logging"
)

// NewRecordCountersTask creates a new splits fetching and storing task
func NewRecordCountersTask(
	synchronizer *synchronizer.MetricSynchronizer,
	period int,
	logger logging.LoggerInterface,
) *asynctask.AsyncTask {
	record := func(logger logging.LoggerInterface) error {
		return synchronizer.SynchronizeCounters()
	}

	onStop := func(l logging.LoggerInterface) {
		record(logger)
	}
	return asynctask.NewAsyncTask("SubmitCounters", record, period, nil, onStop, logger)
}

// NewRecordGaugesTask creates a new splits fetching and storing task
func NewRecordGaugesTask(
	synchronizer *synchronizer.MetricSynchronizer,
	period int,
	logger logging.LoggerInterface,
) *asynctask.AsyncTask {
	record := func(logger logging.LoggerInterface) error {
		return synchronizer.SynchronizeGauges()
	}

	onStop := func(l logging.LoggerInterface) {
		record(logger)
	}

	return asynctask.NewAsyncTask("SubmitGauges", record, period, nil, onStop, logger)
}

// NewRecordLatenciesTask creates a new splits fetching and storing task
func NewRecordLatenciesTask(
	synchronizer *synchronizer.MetricSynchronizer,
	period int,
	logger logging.LoggerInterface,
) *asynctask.AsyncTask {
	record := func(logger logging.LoggerInterface) error {
		return synchronizer.SynchronizeLatencies()
	}

	onStop := func(l logging.LoggerInterface) {
		record(logger)
	}

	return asynctask.NewAsyncTask("SubmitLatencies", record, period, nil, onStop, logger)
}
