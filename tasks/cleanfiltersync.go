package tasks

import (
	"github.com/splitio/go-split-commons/v4/storage"
	"github.com/splitio/go-toolkit/v5/asynctask"
	"github.com/splitio/go-toolkit/v5/logging"
)

const period = 3600 // 24 hours

// NewRecordTelemetryTask creates a new telemtry recording task
func NewCleanFilterTask(
	filter storage.Filter,
	logger logging.LoggerInterface,
) *asynctask.AsyncTask {
	record := func(logger logging.LoggerInterface) error {
		filter.Clear()
		return nil
	}

	onStop := func(l logging.LoggerInterface) {
		filter.Clear()
	}

	return asynctask.NewAsyncTask("SubmitTelemetry", record, period, nil, onStop, logger)
}
