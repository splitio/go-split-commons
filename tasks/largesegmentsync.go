package tasks

import (
	"errors"
	"fmt"
	"sync/atomic"

	hc "github.com/splitio/go-split-commons/v6/healthcheck/application"
	"github.com/splitio/go-split-commons/v6/storage"
	"github.com/splitio/go-split-commons/v6/synchronizer/worker/largesegment"
	"github.com/splitio/go-toolkit/v5/asynctask"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/splitio/go-toolkit/v5/workerpool"
)

func updateLargeSegments(
	splitStorage storage.SplitStorageConsumer,
	admin *workerpool.WorkerAdmin,
	logger logging.LoggerInterface,
) error {
	lsList := splitStorage.LargeSegmentNames().List()
	for _, name := range lsList {
		ok := admin.QueueMessage(name)
		if !ok {
			logger.Error(
				fmt.Sprintf("Large Segment %s could not be added because the job queue is full.\n", name),
				fmt.Sprintf(
					"You currently have %d Large Segments and the queue size is %d.\n",
					len(lsList),
					admin.QueueSize(),
				),
				"Please consider updating the Large Segment queue size accordingly in the configuration options",
			)
		}
	}
	return nil
}

// NewFetchLargeSegmentsTask creates a new large segment fetching and storing task
func NewFetchLargeSegmentsTask(
	fetcher largesegment.Updater,
	splitStorage storage.SplitStorageConsumer,
	period int,
	workerCount int,
	queueSize int,
	logger logging.LoggerInterface,
	appMonitor hc.MonitorProducerInterface,
) *asynctask.AsyncTask {
	admin := atomic.Value{}

	onInit := func(logger logging.LoggerInterface) error {
		admin.Store(workerpool.NewWorkerAdmin(queueSize, logger))
		for i := 0; i < workerCount; i++ {
			worker := NewSegmentWorker(
				fmt.Sprintf("LargeSegmentWorker_%d", i),
				0,
				logger,
				func(n string, t *int64) error {
					_, err := fetcher.SynchronizeLargeSegment(n, t)
					return err
				},
			)
			admin.Load().(*workerpool.WorkerAdmin).AddWorker(worker)
		}

		return nil
	}

	update := func(logger logging.LoggerInterface) error {
		appMonitor.NotifyEvent(hc.LargeSegments)
		wa, ok := admin.Load().(*workerpool.WorkerAdmin)
		if !ok || wa == nil {
			return errors.New("unable to type-assert worker manager")
		}

		return updateLargeSegments(splitStorage, wa, logger)
	}

	cleanup := func(logger logging.LoggerInterface) {
		wa, ok := admin.Load().(*workerpool.WorkerAdmin)
		if !ok || wa == nil {
			logger.Error("unable to type-assert worker manager")
			return
		}
		wa.StopAll(true)
	}

	return asynctask.NewAsyncTask("UpdateLargeSegments", update, period, onInit, cleanup, logger)
}
