package largesegment

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/healthcheck/application"
	"github.com/splitio/go-split-commons/v6/service"
	"github.com/splitio/go-split-commons/v6/storage"
	"github.com/splitio/go-split-commons/v6/synchronizer/worker/segment"
	"github.com/splitio/go-toolkit/v5/backoff"
	"github.com/splitio/go-toolkit/v5/datastructures/set"
	"github.com/splitio/go-toolkit/v5/logging"
	"golang.org/x/sync/semaphore"
)

const (
	onDemandFetchBackoffBase       = int64(10)        // backoff base starting at 10 seconds
	onDemandFetchBackoffMaxWait    = 60 * time.Second //  don't sleep for more than 1 minute
	onDemandFetchBackoffMaxRetries = 5

	maxConcurrency = 10

	// LargeSegmentDefinitionUpdate received when a large segment definition is updated
	LargeSegmentNewDefinition = "LS_NEW_DEFINITION"

	// LargeSegmentEmpty received when a large segment has no definition
	LargeSegmentEmpty = "LS_EMPTY"
)

type internalLargeSegmentSync struct {
	successfulSync bool
	attempt        int
}

// Updater interface
type Updater interface {
	SynchronizeLargeSegment(name string, till *int64) error
	SynchronizeLargeSegments() error
}

// UpdaterImpl struct for segment sync
type UpdaterImpl struct {
	splitStorage                storage.SplitStorageConsumer
	largeSegmentStorage         storage.LargeSegmentsStorage
	largeSegmentFetcher         service.LargeSegmentFetcher
	logger                      logging.LoggerInterface
	runtimeTelemetry            storage.TelemetryRuntimeProducer
	onDemandFetchBackoffBase    int64
	onDemandFetchBackoffMaxWait time.Duration
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
		splitStorage:                splitStorage,
		largeSegmentStorage:         largeSegmentStorage,
		largeSegmentFetcher:         largeSegmentFetcher,
		logger:                      logger,
		runtimeTelemetry:            runtimeTelemetry,
		onDemandFetchBackoffBase:    onDemandFetchBackoffBase,
		onDemandFetchBackoffMaxWait: onDemandFetchBackoffMaxWait,
	}
}

func (u *UpdaterImpl) SynchronizeLargeSegments() error {
	lsNames := u.splitStorage.LargeSegmentNames().List()
	wg := sync.WaitGroup{}
	wg.Add(len(lsNames))
	failedLargeSegments := set.NewThreadSafeSet()

	var mtx sync.Mutex
	errorsToPrint := segment.NewErrors()

	sem := semaphore.NewWeighted(maxConcurrency)
	for _, name := range lsNames {
		conv, ok := name.(string)
		if !ok {
			u.logger.Warning("Skipping non-string large segment present in storage at initialization-time!")
			continue
		}

		go func(segmentName string) {
			sem.Acquire(context.Background(), 1)
			defer sem.Release(1)
			defer wg.Done() // Make sure the "finished" signal is always sent
			err := u.SynchronizeLargeSegment(segmentName, nil)
			if err != nil {
				failedLargeSegments.Add(segmentName)
				errorsToPrint.AddError(segmentName, err)
			}

			mtx.Lock()
			defer mtx.Unlock()
		}(conv)
	}
	wg.Wait()

	if failedLargeSegments.Size() > 0 {
		return fmt.Errorf("the following errors happened when synchronizing large segments: %v", errorsToPrint.Error())
	}

	return nil
}

func (u *UpdaterImpl) SynchronizeLargeSegment(name string, till *int64) error {
	fetchOptions := service.MakeSegmentRequestParams()
	currentSince := u.largeSegmentStorage.ChangeNumber(name)
	if till != nil && *till <= currentSince { // the passed till is less than change_number, no need to perform updates
		return nil
	}

	internalLargeSegmentSync, err := u.attemptLargeSegmentSync(name, fetchOptions)
	if err != nil {
		return err
	}

	if internalLargeSegmentSync.successfulSync {
		attempts := onDemandFetchBackoffMaxRetries - internalLargeSegmentSync.attempt
		u.logger.Debug(fmt.Sprintf("Refresh completed in %d attempts.", attempts))
		return nil
	}

	internalSyncResultCDNBypass, err := u.attemptLargeSegmentSync(name, fetchOptions.WithTill(rand.Int63()))
	if err != nil {
		return err
	}

	withoutCDNattempts := onDemandFetchBackoffMaxRetries - internalSyncResultCDNBypass.attempt
	if internalSyncResultCDNBypass.successfulSync {
		u.logger.Debug(fmt.Sprintf("Refresh completed bypassing the CDN in %d attempts.", withoutCDNattempts))
		return nil
	}

	u.logger.Debug(fmt.Sprintf("No changes fetched after %d attempts with CDN bypassed.", withoutCDNattempts))
	return nil
}

func (u *UpdaterImpl) attemptLargeSegmentSync(name string, fetchOptions *service.SegmentRequestParams) (internalLargeSegmentSync, error) {
	internalBackoff := backoff.New(u.onDemandFetchBackoffBase, u.onDemandFetchBackoffMaxWait)
	remainingAttempts := onDemandFetchBackoffMaxRetries
	for {
		remainingAttempts--
		retry, err := u.attemptSync(name, fetchOptions)

		if !retry {
			return internalLargeSegmentSync{successfulSync: true, attempt: remainingAttempts}, err
		}

		if remainingAttempts <= 0 {
			return internalLargeSegmentSync{successfulSync: false, attempt: remainingAttempts}, err
		}

		howLong := internalBackoff.Next()
		time.Sleep(howLong)
	}
}

func (u *UpdaterImpl) attemptSync(name string, fetchOptions *service.SegmentRequestParams) (bool, error) {
	u.logger.Debug(fmt.Sprintf("Synchronizing large segment %s", name))
	currentSince := u.largeSegmentStorage.ChangeNumber(name)

	lsRFDResponseDTO, err := u.largeSegmentFetcher.Fetch(name, fetchOptions.WithChangeNumber(currentSince))
	if err != nil {
		if httpError, ok := err.(dtos.HTTPError); ok {
			// record sync error telemetry
			if httpError.Code == http.StatusNotModified {
				return false, nil
			}
		}
		return true, err
	}

	return u.processNotificationType(name, lsRFDResponseDTO)
}

func (u *UpdaterImpl) processNotificationType(name string, lsRFDResponseDTO *dtos.LargeSegmentRFDResponseDTO) (bool, error) {
	switch lsRFDResponseDTO.NotificationType {
	case LargeSegmentEmpty:
		// logger debug
		u.logger.Debug(fmt.Sprintf("Processing LargeSegmentEmpty notification for %s", name))
		u.largeSegmentStorage.Update(name, []string{}, lsRFDResponseDTO.ChangeNumber)
		return false, nil
	case LargeSegmentNewDefinition:
		if lsRFDResponseDTO.RFD == nil {
			return false, fmt.Errorf("something went wrong reading RequestForDownload data for %s", name)
		}

		if lsRFDResponseDTO.RFD.Data.ExpiresAt <= time.Now().UnixMilli() {
			u.logger.Warning(fmt.Sprintf("URL expired for %s", name))
			return true, nil
		}

		ls, err := u.largeSegmentFetcher.DownloadFile(name, lsRFDResponseDTO)
		if err != nil {
			u.logger.Warning(err.Error())
			return true, nil
		}

		u.largeSegmentStorage.Update(name, ls.Keys, ls.ChangeNumber)
		return false, nil
	}

	return false, fmt.Errorf("unsopported Notification Type")
}

var _ Updater = (*UpdaterImpl)(nil)
