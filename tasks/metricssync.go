package tasks

import (
	"errors"

	"github.com/splitio/go-split-commons/service"
	"github.com/splitio/go-split-commons/storage"
	"github.com/splitio/go-toolkit/asynctask"
	"github.com/splitio/go-toolkit/logging"
)

func submitCounters(
	metricsStorage storage.MetricsStorageConsumer,
	metricsRecorder service.MetricsRecorder,
) error {
	counters := metricsStorage.PopCounters()
	if len(counters) > 0 {
		err := metricsRecorder.RecordCounters(counters)
		return err
	}
	return nil
}

func submitGauges(
	metricsStorage storage.MetricsStorageConsumer,
	metricsRecorder service.MetricsRecorder,
) error {
	var errs []error
	for _, gauge := range metricsStorage.PopGauges() {
		err := metricsRecorder.RecordGauge(gauge)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.New("Some gauges could not be posted")
	}
	return nil
}

func submitLatencies(
	metricsStorage storage.MetricsStorageConsumer,
	metricsRecorder service.MetricsRecorder,
) error {
	latencies := metricsStorage.PopLatencies()
	if len(latencies) > 0 {
		err := metricsRecorder.RecordLatencies(latencies)
		return err
	}
	return nil
}

// NewRecordCountersTask creates a new splits fetching and storing task
func NewRecordCountersTask(
	metricsStorage storage.MetricsStorageConsumer,
	metricsRecorder service.MetricsRecorder,
	period int,
	logger logging.LoggerInterface,
) *asynctask.AsyncTask {
	record := func(logger logging.LoggerInterface) error {
		return submitCounters(
			metricsStorage,
			metricsRecorder,
		)
	}

	onStop := func(l logging.LoggerInterface) {
		record(logger)
	}
	return asynctask.NewAsyncTask("SubmitCounters", record, period, nil, onStop, logger)
}

// NewRecordGaugesTask creates a new splits fetching and storing task
func NewRecordGaugesTask(
	metricsStorage storage.MetricsStorageConsumer,
	metricsRecorder service.MetricsRecorder,
	period int,
	logger logging.LoggerInterface,
) *asynctask.AsyncTask {
	record := func(logger logging.LoggerInterface) error {
		return submitGauges(
			metricsStorage,
			metricsRecorder,
		)
	}

	onStop := func(l logging.LoggerInterface) {
		record(logger)
	}

	return asynctask.NewAsyncTask("SubmitGauges", record, period, nil, onStop, logger)
}

// NewRecordLatenciesTask creates a new splits fetching and storing task
func NewRecordLatenciesTask(
	metricsStorage storage.MetricsStorageConsumer,
	metricsRecorder service.MetricsRecorder,
	period int,
	logger logging.LoggerInterface,
) *asynctask.AsyncTask {
	record := func(logger logging.LoggerInterface) error {
		return submitLatencies(
			metricsStorage,
			metricsRecorder,
		)
	}

	onStop := func(l logging.LoggerInterface) {
		record(logger)
	}

	return asynctask.NewAsyncTask("SubmitLatencies", record, period, nil, onStop, logger)
}
