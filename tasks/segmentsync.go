package tasks

import (
	"fmt"

	"github.com/splitio/go-split-commons/synchronizer/worker"
	"github.com/splitio/go-toolkit/asynctask"
	"github.com/splitio/go-toolkit/logging"
	"github.com/splitio/go-toolkit/workerpool"
)

func updateSegments(
	fetcher *worker.SegmentFetcher,
	admin *workerpool.WorkerAdmin,
	logger logging.LoggerInterface,
) error {
	segmentList := fetcher.SegmentNames()
	for _, name := range segmentList {
		ok := admin.QueueMessage(name)
		if !ok {
			logger.Error(
				fmt.Sprintf("Segment %s could not be added because the job queue is full.\n", name),
				fmt.Sprintf(
					"You currently have %d segments and the queue size is %d.\n",
					len(segmentList),
					admin.QueueSize(),
				),
				"Please consider updating the segment queue size accordingly in the configuration options",
			)
		}
	}
	return nil
}

// NewFetchSegmentsTask creates a new segment fetching and storing task
func NewFetchSegmentsTask(
	fetcher *worker.SegmentFetcher,
	period int,
	workerCount int,
	queueSize int,
	logger logging.LoggerInterface,
) *asynctask.AsyncTask {
	admin := workerpool.NewWorkerAdmin(queueSize, logger)

	// After all segments are in sync, add workers to the pool that will keep them up to date
	// periodically
	for i := 0; i < workerCount; i++ {
		worker := NewSegmentWorker(
			fmt.Sprintf("SegmentWorker_%d", i),
			0,
			fetcher.SynchronizeSegment,
		)
		admin.AddWorker(worker)
	}

	update := func(logger logging.LoggerInterface) error {
		return updateSegments(fetcher, admin, logger)
	}

	cleanup := func(logger logging.LoggerInterface) {
		admin.StopAll()
	}

	return asynctask.NewAsyncTask("UpdateSegments", update, period, nil, cleanup, logger)
}
