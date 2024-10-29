package largesegment

import (
	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/healthcheck/application"
	"github.com/splitio/go-split-commons/v6/service"
	"github.com/splitio/go-split-commons/v6/storage"
	"github.com/splitio/go-toolkit/v5/logging"
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
	// fetchOptions := service.MakeSegmentRequestParams()

	// currentSince := u.largeSegmentStorage.ChangeNumber(name)
	// if till != nil && *till <= currentSince { // the passed till is less than change_number, no need to perform updates
	// 	return currentSince, nil
	// }

	return 0, nil
}

func (u *UpdaterImpl) fetchUntil(name string, fetchOptions *service.SegmentRequestParams) *dtos.LargeSegmentResponse {
	var currentSince int64
	var response *dtos.LargeSegmentResponse

	for {
		currentSince = u.largeSegmentStorage.ChangeNumber(name)

		response = u.largeSegmentFetcher.Fetch(name, fetchOptions.WithChangeNumber(currentSince))
		if response.Error != nil {
			break
		}

		currentSince = response.Data.ChangeNumber
		u.largeSegmentStorage.Update(name, response.Data.Keys, response.Data.ChangeNumber)

	}

	return response
}
