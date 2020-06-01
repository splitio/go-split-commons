package tasks

import (
	"github.com/splitio/go-split-commons/synchronizer"
	"github.com/splitio/go-toolkit/asynctask"
	"github.com/splitio/go-toolkit/logging"
)

// NewRecordTelemetryTask creates a new telemtry recording task
func NewRecordTelemetryTask(
	synchronizer *synchronizer.MetricSynchronizer,
	period int,
	logger logging.LoggerInterface,
) *asynctask.AsyncTask {
	record := func(logger logging.LoggerInterface) error {
		return synchronizer.SynchronizeTelemetry()
	}

	onStop := func(l logging.LoggerInterface) {
		record(logger)
	}
	return asynctask.NewAsyncTask("SubmitTelemetry", record, period, nil, onStop, logger)
}
