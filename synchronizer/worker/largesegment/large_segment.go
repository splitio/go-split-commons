package largesegment

import (
	"net/http"
	"time"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/healthcheck/application"
	"github.com/splitio/go-split-commons/v6/service"
	"github.com/splitio/go-split-commons/v6/storage"
	"github.com/splitio/go-toolkit/v5/backoff"
	"github.com/splitio/go-toolkit/v5/logging"
)

const (
	onDemandFetchBackoffBase       = int64(10)        // backoff base starting at 10 seconds
	onDemandFetchBackoffMaxWait    = 60 * time.Second //  don't sleep for more than 1 minute
	onDemandFetchBackoffMaxRetries = 10

	maxConcurrency = 10
)

// Updater interface
type Updater interface {
	SynchronizeLargeSegment(name string, till *int64) (int64, error)
	SynchronizeLargeSegments() (map[string]int64, error)
	SegmentNames() []interface{}
}

// UpdaterImpl struct for segment sync
type UpdaterImpl struct {
	splitStorage        storage.SplitStorageConsumer
	largeSegmentStorage storage.LargeSegmentsStorage
	largeSegmentFetcher service.LargeSegmentFetcher
	logger              logging.LoggerInterface
	runtimeTelemetry    storage.TelemetryRuntimeProducer
}

// NewLargeSegmentUpdater creates new large segment synchronizer for processing larrge segment updates
func NewLargeSegmentUpdater(
	splitStorage storage.SplitStorageConsumer,
	largeSegmentStorage storage.LargeSegmentsStorage,
	largeSegmentFetcher service.LargeSegmentFetcher,
	logger logging.LoggerInterface,
	runtimeTelemetry storage.TelemetryRuntimeProducer,
	hcMonitor application.MonitorProducerInterface,
) *UpdaterImpl {
	return &UpdaterImpl{
		splitStorage:        splitStorage,
		largeSegmentStorage: largeSegmentStorage,
		largeSegmentFetcher: largeSegmentFetcher,
		logger:              logger,
		runtimeTelemetry:    runtimeTelemetry,
	}
}

func (u *UpdaterImpl) SynchronizeLargeSegment(name string, till *int64) (int64, error) {
	fetchOptions := service.MakeSegmentRequestParams()

	currentSince := u.largeSegmentStorage.ChangeNumber(name)
	if till != nil && *till <= currentSince { // the passed till is less than change_number, no need to perform updates
		return currentSince, nil
	}

	ok := u.fetch(name, fetchOptions.WithChangeNumber(currentSince))

	return 0, nil
}
func (u *UpdaterImpl) attemptSegmentSync(name string, till *int64, fetchOptions *service.SegmentRequestParams) (bool, error) {
	internalBackoff := backoff.New(onDemandFetchBackoffBase, onDemandFetchBackoffMaxWait)
	remainingAttempts := onDemandFetchBackoffMaxRetries

	for {
		remainingAttempts = remainingAttempts - 1
		retry, err := u.fetchUntil(name, fetchOptions)

	}
}
func (u *UpdaterImpl) fetchUntil(name string, fetchOptions *service.SegmentRequestParams) (bool, error) {
	var currentSince int64
	var retry bool
	var err error

	for {
		currentSince = u.largeSegmentStorage.ChangeNumber(name)

		response := u.largeSegmentFetcher.Fetch(name, fetchOptions.WithChangeNumber(currentSince))
		if response.Error != nil {
			err = response.Error
			if httpError, ok := response.Error.(*dtos.HTTPError); ok {
				if httpError.Code == http.StatusNotModified {
					retry = false
					break
				}
			}

			retry = true
			break
		}

		u.largeSegmentStorage.Update(name, response.Data.Keys, response.Data.ChangeNumber)
		// record telemetry
	}

	return retry, err
}
