package largesegment

import (
	"context"
	"fmt"
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
	onDemandFetchBackoffMaxRetries = 10

	maxConcurrency = 10
)

type internalLargeSegmentSync struct {
	newChangeNumber int64
	successfulSync  bool
	attempt         int
}

// Updater interface
type Updater interface {
	SynchronizeLargeSegment(name string, till *int64) (int64, error)
	SynchronizeLargeSegments() (map[string]int64, error)
	LargeSegmentNames() []interface{}
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

// SegmentNames returns all large segments
func (u *UpdaterImpl) LargeSegmentNames() []interface{} {
	return u.splitStorage.LargeSegmentNames().List()
}

func (u *UpdaterImpl) SynchronizeLargeSegments() (map[string]int64, error) {
	lsNames := u.LargeSegmentNames()
	wg := sync.WaitGroup{}
	wg.Add(len(lsNames))
	failedLargeSegments := set.NewThreadSafeSet()

	var mtx sync.Mutex
	results := make(map[string]int64, len(lsNames))
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
			res, err := u.SynchronizeLargeSegment(segmentName, nil)
			if err != nil {
				failedLargeSegments.Add(segmentName)
				errorsToPrint.AddError(segmentName, err)
			}

			mtx.Lock()
			defer mtx.Unlock()
			results[segmentName] = res
		}(conv)
	}
	wg.Wait()

	if failedLargeSegments.Size() > 0 {
		return results, fmt.Errorf("the following errors happened when synchronizing large segments: %v", errorsToPrint.Error())
	}

	return results, nil
}

func (u *UpdaterImpl) SynchronizeLargeSegment(name string, till *int64) (int64, error) {
	fetchOptions := service.MakeSegmentRequestParams()
	currentSince := u.largeSegmentStorage.ChangeNumber(name)
	if till != nil && *till <= currentSince { // the passed till is less than change_number, no need to perform updates
		return currentSince, nil
	}

	internalLargeSegmentSync, err := u.attemptLargeSegmentSync(name, till, fetchOptions)
	if err != nil {
		return internalLargeSegmentSync.newChangeNumber, err
	}

	attempts := onDemandFetchBackoffMaxRetries - internalLargeSegmentSync.attempt
	if internalLargeSegmentSync.successfulSync {
		u.logger.Debug(fmt.Sprintf("Refresh completed in %d attempts.", attempts))
		return internalLargeSegmentSync.newChangeNumber, nil
	}

	withCDNBypass := service.MakeSegmentRequestParams().WithTill(internalLargeSegmentSync.newChangeNumber) // Set flag for bypassing CDN
	internalSyncResultCDNBypass, err := u.attemptLargeSegmentSync(name, till, withCDNBypass)
	if err != nil {
		return internalLargeSegmentSync.newChangeNumber, err
	}

	withoutCDNattempts := onDemandFetchBackoffMaxRetries - internalSyncResultCDNBypass.attempt
	if internalSyncResultCDNBypass.successfulSync {
		u.logger.Debug(fmt.Sprintf("Refresh completed bypassing the CDN in %d attempts.", withoutCDNattempts))
		return internalSyncResultCDNBypass.newChangeNumber, nil
	}

	u.logger.Debug(fmt.Sprintf("No changes fetched after %d attempts with CDN bypassed.", withoutCDNattempts))
	return internalSyncResultCDNBypass.newChangeNumber, nil
}

func (u *UpdaterImpl) attemptLargeSegmentSync(name string, till *int64, fetchOptions *service.SegmentRequestParams) (internalLargeSegmentSync, error) {
	internalBackoff := backoff.New(u.onDemandFetchBackoffBase, u.onDemandFetchBackoffMaxWait)
	remainingAttempts := onDemandFetchBackoffMaxRetries
	for {
		remainingAttempts--
		cn, err := u.fetchUntil(name, fetchOptions)

		if err != nil || remainingAttempts <= 0 {
			return internalLargeSegmentSync{newChangeNumber: cn, successfulSync: false, attempt: remainingAttempts}, err
		}
		if till == nil || *till <= cn {
			fmt.Println("attemptLargeSegmentSync success")
			fmt.Println(cn)
			return internalLargeSegmentSync{newChangeNumber: cn, successfulSync: true, attempt: remainingAttempts}, nil
		}

		howLong := internalBackoff.Next()
		time.Sleep(howLong)
	}
}

func (u *UpdaterImpl) fetchUntil(name string, fetchOptions *service.SegmentRequestParams) (int64, error) {
	var currentSince int64
	var err error

	for {
		u.logger.Debug(fmt.Sprintf("Synchronizing large segment %s", name))
		currentSince = u.largeSegmentStorage.ChangeNumber(name)

		var rfe *dtos.RfeDTO
		rfe, err = u.largeSegmentFetcher.RequestForExport(name, fetchOptions.WithChangeNumber(currentSince))
		if err != nil {
			if httpError, ok := err.(dtos.HTTPError); ok {
				// record sync error telemetry
				if httpError.Code == http.StatusNotModified {
					err = nil // cleaning error
				}
			}
			break
		}

		var ls *dtos.LargeSegmentDTO
		ls, err = u.largeSegmentFetcher.Fetch(*rfe)
		if err != nil {
			break
		}

		u.largeSegmentStorage.Update(ls.Name, ls.Keys, ls.ChangeNumber)
		// record latency telemetry

		if currentSince == ls.ChangeNumber {
			fmt.Println("cs == ls.cn")
			fmt.Println(currentSince)
			fmt.Println(ls.ChangeNumber)
			// record successful sync telemetry
			break
		}
	}

	return currentSince, err
}

var _ Updater = (*UpdaterImpl)(nil)
