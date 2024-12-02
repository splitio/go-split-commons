package largesegment

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/splitio/go-split-commons/v6/dtos"
	hc "github.com/splitio/go-split-commons/v6/healthcheck/application"
	"github.com/splitio/go-split-commons/v6/service"
	"github.com/splitio/go-split-commons/v6/storage"
	"github.com/splitio/go-split-commons/v6/synchronizer/worker/utils"
	"github.com/splitio/go-toolkit/v5/backoff"
	"github.com/splitio/go-toolkit/v5/datastructures/set"
	"github.com/splitio/go-toolkit/v5/logging"
	tSync "github.com/splitio/go-toolkit/v5/sync"
	"golang.org/x/sync/semaphore"
)

const (
	onDemandFetchBackoffBase       = int64(10)        // backoff base starting at 10 seconds
	onDemandFetchBackoffMaxWait    = 60 * time.Second //  don't sleep for more than 1 minute
	onDemandFetchBackoffMaxRetries = 5

	maxConcurrency = 5

	// LargeSegmentDefinitionUpdate received when a large segment definition is updated
	LargeSegmentNewDefinition = "LS_NEW_DEFINITION"

	// LargeSegmentEmpty received when a large segment has no definition
	LargeSegmentEmpty = "LS_EMPTY"
)

type internalLargeSegmentSync struct {
	successfulSync  bool
	newChangeNumber int64
	attempt         int
}

type internalUpdateResponse struct {
	changeNumber int64
	retry        bool
	err          error
}

// Updater interface
type Updater interface {
	SynchronizeLargeSegment(name string, till *int64) (*int64, error)
	SynchronizeLargeSegmentUpdate(lsRFDResponseDTO *dtos.LargeSegmentRFDResponseDTO) (*int64, error)
	SynchronizeLargeSegments() (map[string]*int64, error)
	IsCached(name string) bool
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
	hcMonitor                   hc.MonitorProducerInterface
	syncInProgress              *tSync.AtomicBool
}

// NewLargeSegmentUpdater creates new large segment synchronizer for processing larrge segment updates
func NewLargeSegmentUpdater(
	splitStorage storage.SplitStorageConsumer,
	largeSegmentStorage storage.LargeSegmentsStorage,
	largeSegmentFetcher service.LargeSegmentFetcher,
	logger logging.LoggerInterface,
	runtimeTelemetry storage.TelemetryRuntimeProducer,
	hcMonitor hc.MonitorProducerInterface,
) *UpdaterImpl {
	return &UpdaterImpl{
		splitStorage:                splitStorage,
		largeSegmentStorage:         largeSegmentStorage,
		largeSegmentFetcher:         largeSegmentFetcher,
		logger:                      logger,
		runtimeTelemetry:            runtimeTelemetry,
		onDemandFetchBackoffBase:    onDemandFetchBackoffBase,
		onDemandFetchBackoffMaxWait: onDemandFetchBackoffMaxWait,
		hcMonitor:                   hcMonitor,
		syncInProgress:              tSync.NewAtomicBool(false),
	}
}

func (s *UpdaterImpl) IsCached(name string) bool {
	cn := s.largeSegmentStorage.ChangeNumber(name)
	return cn != -1
}

func (u *UpdaterImpl) SynchronizeLargeSegments() (map[string]*int64, error) {
	if !u.syncInProgress.TestAndSet() {
		u.logger.Debug("SynchronizeLargeSegments task is already running.")
		return map[string]*int64{}, nil
	}
	defer u.syncInProgress.Unset()

	lsNames := u.splitStorage.LargeSegmentNames().List()
	u.hcMonitor.NotifyEvent(hc.LargeSegments)
	wg := sync.WaitGroup{}
	wg.Add(len(lsNames))
	failedLargeSegments := set.NewThreadSafeSet()

	var mtx sync.Mutex
	results := make(map[string]*int64, len(lsNames))
	errorsToPrint := utils.NewErrors()

	sem := semaphore.NewWeighted(maxConcurrency)
	for _, name := range lsNames {
		conv, ok := name.(string)
		if !ok {
			u.logger.Warning("Skipping non-string large segment present in storage at initialization-time!")
			continue
		}

		u.initLargeSegment(conv)
		go func(name string) {
			sem.Acquire(context.Background(), 1)
			defer sem.Release(1)
			defer wg.Done() // Make sure the "finished" signal is always sent
			cn, err := u.SynchronizeLargeSegment(name, nil)
			if err != nil {
				failedLargeSegments.Add(name)
				errorsToPrint.AddError(name, err)
			}

			mtx.Lock()
			defer mtx.Unlock()
			results[name] = cn
		}(conv)
	}
	wg.Wait()

	if failedLargeSegments.Size() > 0 {
		return results, fmt.Errorf("the following errors happened when synchronizing large segments: %v", errorsToPrint.Error())
	}

	return results, nil
}

func (u *UpdaterImpl) SynchronizeLargeSegment(name string, till *int64) (*int64, error) {
	fetchOptions := service.MakeSegmentRequestParams()
	u.hcMonitor.NotifyEvent(hc.LargeSegments)

	currentSince := u.largeSegmentStorage.ChangeNumber(name)
	if till != nil && *till <= currentSince { // the passed till is less than change_number, no need to perform updates
		return nil, nil
	}

	internalLargeSegmentSync, err := u.attemptLargeSegmentSync(name, fetchOptions)
	if err != nil {
		return nil, err
	}

	if internalLargeSegmentSync.successfulSync {
		attempts := onDemandFetchBackoffMaxRetries - internalLargeSegmentSync.attempt
		u.logger.Debug(fmt.Sprintf("Refresh completed in %d attempts for %s.", attempts, name))
		return &internalLargeSegmentSync.newChangeNumber, nil
	}

	withCDNBypass := service.MakeSegmentRequestParams().WithTill(time.Now().UnixMilli()) // Set flag for bypassing CDN
	internalSyncResultCDNBypass, err := u.attemptLargeSegmentSync(name, withCDNBypass)
	if err != nil {
		return nil, err
	}

	withoutCDNattempts := onDemandFetchBackoffMaxRetries - internalSyncResultCDNBypass.attempt
	if internalSyncResultCDNBypass.successfulSync {
		u.logger.Debug(fmt.Sprintf("Refresh completed bypassing the CDN in %d attempts.", withoutCDNattempts))
		return &internalSyncResultCDNBypass.newChangeNumber, nil
	}

	u.logger.Debug(fmt.Sprintf("No changes fetched after %d attempts with CDN bypassed.", withoutCDNattempts))
	return &internalSyncResultCDNBypass.newChangeNumber, nil
}

func (u *UpdaterImpl) SynchronizeLargeSegmentUpdate(lsRFDResponseDTO *dtos.LargeSegmentRFDResponseDTO) (*int64, error) {
	resp := u.processUpdate(lsRFDResponseDTO)

	if resp.retry {
		return u.SynchronizeLargeSegment(lsRFDResponseDTO.Name, nil)
	}

	return &resp.changeNumber, resp.err
}

func (u *UpdaterImpl) attemptLargeSegmentSync(name string, fetchOptions *service.SegmentRequestParams) (internalLargeSegmentSync, error) {
	internalBackoff := backoff.New(u.onDemandFetchBackoffBase, u.onDemandFetchBackoffMaxWait)
	remainingAttempts := onDemandFetchBackoffMaxRetries
	for {
		remainingAttempts--
		resp := u.attemptSync(name, fetchOptions)

		if !resp.retry {
			return internalLargeSegmentSync{newChangeNumber: resp.changeNumber, successfulSync: true, attempt: remainingAttempts}, resp.err
		}

		if remainingAttempts <= 0 {
			return internalLargeSegmentSync{newChangeNumber: resp.changeNumber, successfulSync: false, attempt: remainingAttempts}, resp.err
		}

		howLong := internalBackoff.Next()
		time.Sleep(howLong)
	}
}

func (u *UpdaterImpl) attemptSync(name string, fetchOptions *service.SegmentRequestParams) internalUpdateResponse {
	u.logger.Debug(fmt.Sprintf("Synchronizing large segment %s", name))
	currentSince := u.largeSegmentStorage.ChangeNumber(name)

	lsRFDResponseDTO, err := u.largeSegmentFetcher.Fetch(name, fetchOptions.WithChangeNumber(currentSince))

	if err != nil {
		if httpError, ok := err.(*dtos.HTTPError); ok {
			// TODO(maurosanz): record sync error telemetry
			if httpError.Code == http.StatusNotModified {
				return internalUpdateResponse{changeNumber: fetchOptions.ChangeNumber(), retry: false}
			}
		}
		return internalUpdateResponse{changeNumber: fetchOptions.ChangeNumber(), retry: true, err: err}
	}

	return u.processUpdate(lsRFDResponseDTO)
}

func (u *UpdaterImpl) processUpdate(lsRFDResponseDTO *dtos.LargeSegmentRFDResponseDTO) internalUpdateResponse {
	switch lsRFDResponseDTO.NotificationType {
	case LargeSegmentEmpty:
		u.logger.Debug(fmt.Sprintf("Processing LargeSegmentEmpty notification for %s", lsRFDResponseDTO.Name))
		u.largeSegmentStorage.Update(lsRFDResponseDTO.Name, []string{}, lsRFDResponseDTO.ChangeNumber)
		return internalUpdateResponse{changeNumber: lsRFDResponseDTO.ChangeNumber, retry: false}
	case LargeSegmentNewDefinition:
		if lsRFDResponseDTO.RFD == nil {
			return internalUpdateResponse{
				changeNumber: lsRFDResponseDTO.ChangeNumber,
				retry:        true,
				err:          fmt.Errorf("something went wrong reading RequestForDownload data for %s", lsRFDResponseDTO.Name),
			}
		}

		if lsRFDResponseDTO.RFD.Data.ExpiresAt <= time.Now().UnixMilli() {
			u.logger.Warning(fmt.Sprintf("URL expired for %s", lsRFDResponseDTO.Name))
			return internalUpdateResponse{changeNumber: lsRFDResponseDTO.ChangeNumber, retry: true}
		}

		data := lsRFDResponseDTO.RFD.Data
		before := time.Now()
		u.logger.Debug(fmt.Sprintf("Downloading LargeSegment file for %s.Size=%d,Format=%d,TotalKeys=%d,V=%s", lsRFDResponseDTO.Name, data.FileSize, data.Format, data.TotalKeys, lsRFDResponseDTO.SpecVersion))

		ls, err := u.largeSegmentFetcher.DownloadFile(lsRFDResponseDTO.Name, lsRFDResponseDTO)
		if err != nil {
			u.logger.Warning(fmt.Sprintf("%s. %s", lsRFDResponseDTO.RFD.Params.URL, err.Error()))
			return internalUpdateResponse{changeNumber: lsRFDResponseDTO.ChangeNumber, retry: true}
		}

		duration := time.Since(before).Seconds()
		u.logger.Debug(fmt.Sprintf("Successfully downloaded and parsed the Large Segment file for %s. Keys=%d,CN=%d,Time=%vs", lsRFDResponseDTO.Name, len(ls.Keys), ls.ChangeNumber, duration))

		u.largeSegmentStorage.Update(lsRFDResponseDTO.Name, ls.Keys, ls.ChangeNumber)
		return internalUpdateResponse{changeNumber: ls.ChangeNumber, retry: false}
	}

	return internalUpdateResponse{
		changeNumber: lsRFDResponseDTO.ChangeNumber,
		retry:        false,
		err:          fmt.Errorf("unsopported Notification Type"),
	}
}

func (u *UpdaterImpl) initLargeSegment(name string) {
	if u.IsCached(name) {
		return
	}

	u.largeSegmentStorage.Update(name, []string{}, -1)
}

var _ Updater = (*UpdaterImpl)(nil)
