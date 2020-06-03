package tasks

import (
	"github.com/splitio/go-split-commons/synchronizer/worker"
	"github.com/splitio/go-toolkit/asynctask"
	"github.com/splitio/go-toolkit/logging"
)

// NewRecordTelemetryTask creates a new telemtry recording task
func NewRecordTelemetryTask(
	recorder *worker.MetricRecorder,
	period int,
	logger logging.LoggerInterface,
) *asynctask.AsyncTask {
	record := func(logger logging.LoggerInterface) error {
		return recorder.SynchronizeTelemetry()
	}

	onStop := func(l logging.LoggerInterface) {
		record(logger)
	}
	return asynctask.NewAsyncTask("SubmitTelemetry", record, period, nil, onStop, logger)
}
