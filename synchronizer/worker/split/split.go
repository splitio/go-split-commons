package split

import (
	"fmt"
	"time"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/engine/validator"
	"github.com/splitio/go-split-commons/v6/flagsets"
	"github.com/splitio/go-split-commons/v6/healthcheck/application"
	"github.com/splitio/go-split-commons/v6/service"
	"github.com/splitio/go-split-commons/v6/storage"
	"github.com/splitio/go-split-commons/v6/synchronizer/worker/rulebasedsegment"
	"github.com/splitio/go-split-commons/v6/telemetry"
	"github.com/splitio/go-toolkit/v5/backoff"
	"github.com/splitio/go-toolkit/v5/common"
	"github.com/splitio/go-toolkit/v5/logging"
)

const (
	matcherTypeInSegment           = "IN_SEGMENT"
	matcherTypeInLargeSegment      = "IN_LARGE_SEGMENT"
	matcherTypeInRuleBasedSegment  = "IN_RULE_BASED_SEGMENT"
	scRequestURITooLong            = 414
	onDemandFetchBackoffBase       = int64(10)        // backoff base starting at 10 seconds
	onDemandFetchBackoffMaxWait    = 60 * time.Second //  don't sleep for more than 1 minute
	onDemandFetchBackoffMaxRetries = 10
)

const (
	Active   = "ACTIVE"
	Archived = "ARCHIVED"
)

// Updater interface
type Updater interface {
	SynchronizeSplits(till *int64) (*UpdateResult, error)
	SynchronizeFeatureFlags(ffChange *dtos.SplitChangeUpdate) (*UpdateResult, error)
	LocalKill(splitName string, defaultTreatment string, changeNumber int64)
}

// UpdateResult encapsulates information regarding the split update performed
type UpdateResult struct {
	UpdatedSplits           []string
	ReferencedSegments      []string
	ReferencedLargeSegments []string
	NewChangeNumber         int64
	NewRBChangeNumber       int64
	RequiresFetch           bool
}

type internalSplitSync struct {
	updateResult   *UpdateResult
	successfulSync bool
	attempt        int
}

// UpdaterImpl struct for split sync
type UpdaterImpl struct {
	splitStorage                storage.SplitStorage
	splitFetcher                service.SplitFetcher
	ruleBasedSegmentStorage     storage.RuleBasedSegmentsStorage
	ruleBasedSegmentUpdater     rulebasedsegment.UpdaterImpl
	logger                      logging.LoggerInterface
	runtimeTelemetry            storage.TelemetryRuntimeProducer
	hcMonitor                   application.MonitorProducerInterface
	onDemandFetchBackoffBase    int64
	onDemandFetchBackoffMaxWait time.Duration
	flagSetsFilter              flagsets.FlagSetFilter
}

// NewSplitUpdater creates new split synchronizer for processing split updates
func NewSplitUpdater(
	splitStorage storage.SplitStorage,
	ruleBasedSegmentStorage storage.RuleBasedSegmentsStorage,
	ruleBasedSegmentUpdater rulebasedsegment.UpdaterImpl,
	splitFetcher service.SplitFetcher,
	logger logging.LoggerInterface,
	runtimeTelemetry storage.TelemetryRuntimeProducer,
	hcMonitor application.MonitorProducerInterface,
	flagSetsFilter flagsets.FlagSetFilter,
) *UpdaterImpl {
	return &UpdaterImpl{
		splitStorage:                splitStorage,
		splitFetcher:                splitFetcher,
		logger:                      logger,
		runtimeTelemetry:            runtimeTelemetry,
		hcMonitor:                   hcMonitor,
		onDemandFetchBackoffBase:    onDemandFetchBackoffBase,
		onDemandFetchBackoffMaxWait: onDemandFetchBackoffMaxWait,
		flagSetsFilter:              flagSetsFilter,
		ruleBasedSegmentStorage:     ruleBasedSegmentStorage,
		ruleBasedSegmentUpdater:     ruleBasedSegmentUpdater,
	}
}

func (s *UpdaterImpl) SetRuleBasedSegmentStorage(storage storage.RuleBasedSegmentsStorage) {
	s.ruleBasedSegmentStorage = storage
}

func (s *UpdaterImpl) processUpdate(splitChanges *dtos.SplitChangesDTO) {
	activeSplits, inactiveSplits := s.processFeatureFlagChanges(splitChanges)
	// Add/Update active splits
	s.splitStorage.Update(activeSplits, inactiveSplits, splitChanges.FeatureFlags.Till)
}

// fetchUntil Hit endpoint, update storage and return when since==till.
func (s *UpdaterImpl) fetchUntil(fetchOptions *service.FlagRequestParams) (*UpdateResult, error) {
	// just guessing sizes so the we don't realloc immediately
	segmentReferences := make([]string, 0, 10)
	updatedSplitNames := make([]string, 0, 10)
	largeSegmentReferences := make([]string, 0, 10)
	var err error
	var currentSince int64
	var currentRBSince int64

	for { // Fetch until since==till
		currentSince, _ = s.splitStorage.ChangeNumber()
		currentRBSince = s.ruleBasedSegmentStorage.ChangeNumber()
		before := time.Now()
		var splitChanges *dtos.SplitChangesDTO
		splitChanges, err = s.splitFetcher.Fetch(fetchOptions.WithChangeNumber(currentSince).WithChangeNumberRB(currentRBSince))
		if err != nil {
			if httpError, ok := err.(*dtos.HTTPError); ok {
				if httpError.Code == scRequestURITooLong {
					s.logger.Error("SDK Initialization, the amount of flag sets provided are big causing uri length error.")
				}
				s.runtimeTelemetry.RecordSyncError(telemetry.SplitSync, httpError.Code)
			}
			break
		}
		currentSince = splitChanges.FeatureFlags.Till
		currentRBSince = splitChanges.RuleBasedSegments.Till
		s.runtimeTelemetry.RecordSyncLatency(telemetry.SplitSync, time.Since(before))
		s.processUpdate(splitChanges)
		segmentReferences = s.ruleBasedSegmentUpdater.ProcessUpdate(splitChanges)
		segmentReferences = appendSegmentNames(segmentReferences, splitChanges)
		updatedSplitNames = appendSplitNames(updatedSplitNames, splitChanges)
		largeSegmentReferences = appendLargeSegmentNames(largeSegmentReferences, splitChanges)
		if currentSince == splitChanges.FeatureFlags.Since && currentRBSince == splitChanges.RuleBasedSegments.Since {
			s.runtimeTelemetry.RecordSuccessfulSync(telemetry.SplitSync, time.Now().UTC())
			break
		}
	}
	return &UpdateResult{
		UpdatedSplits:           common.DedupeStringSlice(updatedSplitNames),
		ReferencedSegments:      common.DedupeStringSlice(segmentReferences),
		NewChangeNumber:         currentSince,
		NewRBChangeNumber:       currentRBSince,
		ReferencedLargeSegments: common.DedupeStringSlice(largeSegmentReferences),
	}, err
}

// attemptSplitSync Hit endpoint, update storage and return True if sync is complete.
func (s *UpdaterImpl) attemptSplitSync(fetchOptions *service.FlagRequestParams, till *int64) (internalSplitSync, error) {
	internalBackoff := backoff.New(s.onDemandFetchBackoffBase, s.onDemandFetchBackoffMaxWait)
	remainingAttempts := onDemandFetchBackoffMaxRetries
	for {
		remainingAttempts = remainingAttempts - 1
		updateResult, err := s.fetchUntil(fetchOptions) // what we should do with err
		if err != nil || remainingAttempts <= 0 {
			return internalSplitSync{updateResult: updateResult, successfulSync: false, attempt: remainingAttempts}, err
		}
		if till == nil || *till <= updateResult.NewChangeNumber || *till <= updateResult.NewRBChangeNumber {
			return internalSplitSync{updateResult: updateResult, successfulSync: true, attempt: remainingAttempts}, nil
		}
		howLong := internalBackoff.Next()
		time.Sleep(howLong)
	}
}

func (s *UpdaterImpl) SynchronizeSplits(till *int64) (*UpdateResult, error) {
	s.hcMonitor.NotifyEvent(application.Splits)
	currentSince, _ := s.splitStorage.ChangeNumber()
	currentRBSince := s.ruleBasedSegmentStorage.ChangeNumber()
	if till != nil && *till < currentSince && *till < currentRBSince { // the passed till is less than change_number, no need to perform updates
		return &UpdateResult{}, nil
	}

	fetchOptions := service.MakeFlagRequestParams()
	internalSyncResult, err := s.attemptSplitSync(fetchOptions, till)
	attempts := onDemandFetchBackoffMaxRetries - internalSyncResult.attempt
	if err != nil {
		return internalSyncResult.updateResult, err
	}
	if internalSyncResult.successfulSync {
		s.logger.Debug(fmt.Sprintf("Refresh completed in %d attempts.", attempts))
		return internalSyncResult.updateResult, nil
	}
	withCDNBypass := service.MakeFlagRequestParams().WithTill(internalSyncResult.updateResult.NewChangeNumber) // Set flag for bypassing CDN
	internalSyncResultCDNBypass, err := s.attemptSplitSync(withCDNBypass, till)
	withoutCDNattempts := onDemandFetchBackoffMaxRetries - internalSyncResultCDNBypass.attempt
	if err != nil {
		return internalSyncResultCDNBypass.updateResult, err
	}
	if internalSyncResultCDNBypass.successfulSync {
		s.logger.Debug(fmt.Sprintf("Refresh completed bypassing the CDN in %d attempts.", withoutCDNattempts))
		return internalSyncResultCDNBypass.updateResult, nil
	}
	s.logger.Debug(fmt.Sprintf("No changes fetched after %d attempts with CDN bypassed.", withoutCDNattempts))
	return internalSyncResultCDNBypass.updateResult, nil
}

func appendSplitNames(dst []string, splitChanges *dtos.SplitChangesDTO) []string {
	for idx := range splitChanges.FeatureFlags.Splits {
		dst = append(dst, splitChanges.FeatureFlags.Splits[idx].Name)
	}
	return dst
}

func appendSegmentNames(dst []string, splitChanges *dtos.SplitChangesDTO) []string {
	for _, split := range splitChanges.FeatureFlags.Splits {
		for _, cond := range split.Conditions {
			for _, matcher := range cond.MatcherGroup.Matchers {
				if matcher.MatcherType == matcherTypeInSegment && matcher.UserDefinedSegment != nil {
					dst = append(dst, matcher.UserDefinedSegment.SegmentName)
				}
			}
		}
	}
	return dst
}

func appendLargeSegmentNames(dst []string, splitChanges *dtos.SplitChangesDTO) []string {
	for _, split := range splitChanges.FeatureFlags.Splits {
		for _, cond := range split.Conditions {
			for _, matcher := range cond.MatcherGroup.Matchers {
				if matcher.MatcherType == matcherTypeInLargeSegment && matcher.UserDefinedLargeSegment != nil {
					dst = append(dst, matcher.UserDefinedLargeSegment.LargeSegmentName)
				}
			}
		}
	}
	return dst
}

func addIfNotExists(dst []string, seen map[string]struct{}, name string) []string {
	if _, exists := seen[name]; !exists {
		seen[name] = struct{}{}
		dst = append(dst, name)
	}
	return dst
}

func appendRuleBasedSegmentNames(dst []string, splitChanges *dtos.SplitChangesDTO) []string {
	seen := make(map[string]struct{})
	// Inicializamos el mapa con lo que ya tiene dst para no duplicar tampoco ahí
	for _, name := range dst {
		seen[name] = struct{}{}
	}

	for _, split := range splitChanges.FeatureFlags.Splits {
		for _, cond := range split.Conditions {
			for _, matcher := range cond.MatcherGroup.Matchers {
				if matcher.MatcherType == matcherTypeInRuleBasedSegment && matcher.UserDefinedSegment != nil {
					dst = addIfNotExists(dst, seen, matcher.UserDefinedSegment.SegmentName)
				}
			}
		}
	}
	return dst
}

func (s *UpdaterImpl) processFeatureFlagChanges(splitChanges *dtos.SplitChangesDTO) ([]dtos.SplitDTO, []dtos.SplitDTO) {
	toRemove := make([]dtos.SplitDTO, 0, len(splitChanges.FeatureFlags.Splits))
	toAdd := make([]dtos.SplitDTO, 0, len(splitChanges.FeatureFlags.Splits))
	for idx := range splitChanges.FeatureFlags.Splits {
		if splitChanges.FeatureFlags.Splits[idx].Status == Active && s.flagSetsFilter.Instersect(splitChanges.FeatureFlags.Splits[idx].Sets) {
			validator.ProcessMatchers(&splitChanges.FeatureFlags.Splits[idx], s.logger)
			toAdd = append(toAdd, splitChanges.FeatureFlags.Splits[idx])
		} else {
			toRemove = append(toRemove, splitChanges.FeatureFlags.Splits[idx])
		}
	}
	return toAdd, toRemove
}

// LocalKill marks a spit as killed in local storage
func (s *UpdaterImpl) LocalKill(splitName string, defaultTreatment string, changeNumber int64) {
	s.splitStorage.KillLocally(splitName, defaultTreatment, changeNumber)
}

func (s *UpdaterImpl) processFFChange(ffChange dtos.SplitChangeUpdate) *UpdateResult {
	changeNumber, err := s.splitStorage.ChangeNumber()
	if err != nil {
		s.logger.Debug(fmt.Sprintf("problem getting change number from feature flag storage: %s", err.Error()))
		return &UpdateResult{RequiresFetch: true}
	}
	if changeNumber >= ffChange.BaseUpdate.ChangeNumber() {
		s.logger.Debug("the feature flag it's already updated")
		return &UpdateResult{RequiresFetch: false}
	}
	if ffChange.FeatureFlag() == nil {
		s.logger.Debug("the feature flag was nil")
		return &UpdateResult{RequiresFetch: true}
	}

	// If we have a feature flag, update it
	segmentReferences := make([]string, 0, 10)
	updatedSplitNames := make([]string, 0, 1)
	largeSegmentReferences := make([]string, 0, 10)
	ruleBasedSegmentReferences := make([]string, 0, 10)
	s.logger.Debug(fmt.Sprintf("updating feature flag %s", ffChange.FeatureFlag().Name))
	featureFlags := make([]dtos.SplitDTO, 0, 1)
	featureFlags = append(featureFlags, *ffChange.FeatureFlag())
	featureFlagChange := dtos.SplitChangesDTO{FeatureFlags: dtos.FeatureFlagsDTO{Splits: featureFlags}}
	activeFFs, inactiveFFs := s.processFeatureFlagChanges(&featureFlagChange)
	s.splitStorage.Update(activeFFs, inactiveFFs, ffChange.BaseUpdate.ChangeNumber())
	s.runtimeTelemetry.RecordUpdatesFromSSE(telemetry.SplitUpdate)
	updatedSplitNames = append(updatedSplitNames, ffChange.FeatureFlag().Name)
	segmentReferences = appendSegmentNames(segmentReferences, &featureFlagChange)
	largeSegmentReferences = appendLargeSegmentNames(largeSegmentReferences, &featureFlagChange)
	ruleBasedSegmentReferences = appendRuleBasedSegmentNames(ruleBasedSegmentReferences, &featureFlagChange)
	requiresFetch := false
	if len(ruleBasedSegmentReferences) > 0 && !s.ruleBasedSegmentStorage.Contains(ruleBasedSegmentReferences) {
		requiresFetch = true
	}
	return &UpdateResult{
		UpdatedSplits:           updatedSplitNames,
		ReferencedSegments:      segmentReferences,
		NewChangeNumber:         ffChange.BaseUpdate.ChangeNumber(),
		RequiresFetch:           requiresFetch,
		ReferencedLargeSegments: largeSegmentReferences,
	}
}

func (s *UpdaterImpl) SynchronizeFeatureFlags(ffChange *dtos.SplitChangeUpdate) (*UpdateResult, error) {
	result := s.processFFChange(*ffChange)
	if result.RequiresFetch {
		return s.SynchronizeSplits(common.Int64Ref(ffChange.BaseUpdate.ChangeNumber()))
	}
	return result, nil
}
