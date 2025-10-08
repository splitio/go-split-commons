package split

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/splitio/go-split-commons/v7/dtos"
	"github.com/splitio/go-split-commons/v7/engine/grammar"
	"github.com/splitio/go-split-commons/v7/engine/validator"
	"github.com/splitio/go-split-commons/v7/flagsets"
	"github.com/splitio/go-split-commons/v7/healthcheck/application"
	"github.com/splitio/go-split-commons/v7/service"
	"github.com/splitio/go-split-commons/v7/service/api/specs"
	"github.com/splitio/go-split-commons/v7/storage"
	"github.com/splitio/go-split-commons/v7/telemetry"
	"github.com/splitio/go-toolkit/v5/backoff"
	"github.com/splitio/go-toolkit/v5/common"
	"github.com/splitio/go-toolkit/v5/logging"
)

const (
	matcherTypeInSegment           = "IN_SEGMENT"
	matcherTypeInLargeSegment      = "IN_LARGE_SEGMENT"
	matcherTypeInRuleBasedSegment  = "IN_RULE_BASED_SEGMENT"
	UpdateTypeSplitChange          = "SPLIT_UPDATE"
	UpdateTypeRuleBasedChange      = "RB_SEGMENT_UPDATE"
	TypeStandard                   = "standard"
	onDemandFetchBackoffBase       = int64(10)        // backoff base starting at 10 seconds
	onDemandFetchBackoffMaxWait    = 60 * time.Second //  don't sleep for more than 1 minute
	onDemandFetchBackoffMaxRetries = 10
)

const (
	Active   = "ACTIVE"
	Archived = "ARCHIVED"
)

var (
	ErrProxy = fmt.Errorf("maybe proxy error")
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
	logger                      logging.LoggerInterface
	runtimeTelemetry            storage.TelemetryRuntimeProducer
	hcMonitor                   application.MonitorProducerInterface
	onDemandFetchBackoffBase    int64
	onDemandFetchBackoffMaxWait time.Duration
	flagSetsFilter              flagsets.FlagSetFilter
	validator                   validator.Validator
	timestampLastSync           int64
	fetcher                     func(fetchOptions *service.FlagRequestParams) (fetchResult, error)
}

// NewSplitUpdater creates new split synchronizer for processing split updates
func NewSplitUpdater(
	splitStorage storage.SplitStorage,
	ruleBasedSegmentStorage storage.RuleBasedSegmentsStorage,
	splitFetcher service.SplitFetcher,
	logger logging.LoggerInterface,
	runtimeTelemetry storage.TelemetryRuntimeProducer,
	hcMonitor application.MonitorProducerInterface,
	flagSetsFilter flagsets.FlagSetFilter,
	ruleBuilder grammar.RuleBuilder,
	isProxy bool,
) *UpdaterImpl {
	updater := &UpdaterImpl{
		splitStorage:                splitStorage,
		splitFetcher:                splitFetcher,
		logger:                      logger,
		runtimeTelemetry:            runtimeTelemetry,
		hcMonitor:                   hcMonitor,
		onDemandFetchBackoffBase:    onDemandFetchBackoffBase,
		onDemandFetchBackoffMaxWait: onDemandFetchBackoffMaxWait,
		flagSetsFilter:              flagSetsFilter,
		ruleBasedSegmentStorage:     ruleBasedSegmentStorage,
		validator:                   validator.NewValidator(ruleBuilder),
		timestampLastSync:           0,
	}
	if !isProxy {
		updater.fetcher = updater.performFetchWithoutProxy
	} else {
		updater.fetcher = updater.performFetch
	}
	return updater
}

func (s *UpdaterImpl) SetRuleBasedSegmentStorage(storage storage.RuleBasedSegmentsStorage) {
	s.ruleBasedSegmentStorage = storage
}

func (s *UpdaterImpl) processUpdate(splitChanges *dtos.SplitChangesDTO) {
	activeSplits, inactiveSplits := s.processFeatureFlagChanges(splitChanges)
	// Add/Update active splits
	s.splitStorage.Update(activeSplits, inactiveSplits, splitChanges.FeatureFlags.Till)
}

func (s *UpdaterImpl) processRuleBasedUpdate(splitChanges *dtos.SplitChangesDTO) []string {
	activeRB, inactiveRB, segments := s.processRuleBasedSegmentChanges(splitChanges)
	// Add/Update active splits
	s.ruleBasedSegmentStorage.Update(activeRB, inactiveRB, splitChanges.RuleBasedSegments.Till)
	return segments
}

type fetchResult struct {
	segmentReferences      []string
	largeSegmentReferences []string
	updatedSplitNames      []string
	ffCurrentSince         int64
	rbCurrentSince         int64
	ffSince                int64
	rbSince                int64
}

func (s *UpdaterImpl) shouldUseLatest() bool {
	return time.Since(time.Unix(s.timestampLastSync, 0)) >= 24*time.Hour
}

func (s *UpdaterImpl) fetch(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
	var splitChanges *dtos.SplitChangesDTO
	var err error
	before := time.Now()
	splitChanges, err = s.splitFetcher.Fetch(fetchOptions)
	if err != nil {
		if httpError, ok := err.(*dtos.HTTPError); ok {
			if httpError.Code == http.StatusRequestURITooLong {
				s.logger.Error("SDK Initialization, the amount of flag sets provided are big causing uri length error.")
			}
			if httpError.Code == http.StatusBadRequest {
				return splitChanges, ErrProxy
			}
			s.runtimeTelemetry.RecordSyncError(telemetry.SplitSync, httpError.Code)
		}
		return splitChanges, err
	}
	s.runtimeTelemetry.RecordSyncLatency(telemetry.SplitSync, time.Since(before))
	return splitChanges, nil
}

func (s *UpdaterImpl) processSplitChanges(splitChanges *dtos.SplitChangesDTO) fetchResult {
	s.processUpdate(splitChanges)
	segmentReferences := s.processRuleBasedUpdate(splitChanges)
	segmentReferences = appendSegmentNames(segmentReferences, splitChanges)
	return fetchResult{
		segmentReferences:      segmentReferences,
		largeSegmentReferences: appendLargeSegmentNames([]string{}, splitChanges),
		updatedSplitNames:      appendSplitNames([]string{}, splitChanges),
		ffCurrentSince:         splitChanges.FeatureFlags.Till,
		rbCurrentSince:         splitChanges.RuleBasedSegments.Till,
		ffSince:                splitChanges.FeatureFlags.Since,
		rbSince:                splitChanges.RuleBasedSegments.Since,
	}
}

func (s *UpdaterImpl) v11(fetchOptions *service.FlagRequestParams, currentSince, currentRBSince int64) (fetchResult, error) {
	splitChanges, err := s.fetch(fetchOptions.Copy().WithSpecVersion(common.StringRef(specs.FLAG_V1_1)).WithChangeNumber(currentSince).WithChangeNumberRB(currentRBSince))
	if err != nil {
		return fetchResult{
			ffSince: currentSince,
			rbSince: currentRBSince,
		}, err
	}
	return s.processSplitChanges(splitChanges), nil
}

func (s *UpdaterImpl) performFetchWithoutProxy(fetchOptions *service.FlagRequestParams) (fetchResult, error) {
	currentSince, _ := s.splitStorage.ChangeNumber()
	currentRBSince := s.ruleBasedSegmentStorage.ChangeNumber()
	splitChanges, err := s.fetch(fetchOptions.Copy().WithSpecVersion(common.StringRef(specs.FLAG_V1_3)).WithChangeNumber(currentSince).WithChangeNumberRB(currentRBSince))
	if err != nil {
		return fetchResult{
			ffSince: currentSince,
			rbSince: currentRBSince,
		}, err
	}
	return s.processSplitChanges(splitChanges), nil
}

func (s *UpdaterImpl) performFetch(fetchOptions *service.FlagRequestParams) (fetchResult, error) {
	var splitChanges *dtos.SplitChangesDTO
	var err error
	currentSince, _ := s.splitStorage.ChangeNumber()
	currentRBSince := s.ruleBasedSegmentStorage.ChangeNumber()
	shouldRetry := s.shouldUseLatest()
	if shouldRetry || s.timestampLastSync == 0 {
		if shouldRetry { // check if timestamp passed and reset since to -1
			currentSince = -1
			currentRBSince = -1
		}
		splitChanges, err = s.fetch(fetchOptions.Copy().WithSpecVersion(common.StringRef(specs.FLAG_V1_3)).WithChangeNumber(currentSince).WithChangeNumberRB(currentRBSince))
		if err != nil {
			if errors.Is(err, ErrProxy) {
				s.timestampLastSync = time.Now().Unix()
				return s.v11(fetchOptions, currentSince, currentRBSince)
			}
			return fetchResult{
				ffSince: currentSince,
				rbSince: currentRBSince,
			}, err
		}
		if shouldRetry { // fetch was ok and needs to replace entire cache because legacy data are stored
			// replaceALL if s.Timestamp != 0 AND RETURN
			// return s.replaceAllChanges(splitChanges), nil
		}
		return s.processSplitChanges(splitChanges), nil
	}
	return s.v11(fetchOptions, currentSince, currentRBSince)
}

// fetchUntil Hit endpoint, update storage and return when since==till.
func (s *UpdaterImpl) fetchUntil(fetchOptions *service.FlagRequestParams) (*UpdateResult, error) {
	// just guessing sizes so the we don't realloc immediately
	segmentReferences := make([]string, 0, 10)
	updatedSplitNames := make([]string, 0, 10)
	largeSegmentReferences := make([]string, 0, 10)
	var err error
	var fetchResult fetchResult

	for { // Fetch until since==till
		fetchResult, err = s.fetcher(fetchOptions)
		if err != nil {
			break
		}
		segmentReferences = append(segmentReferences, fetchResult.segmentReferences...)
		largeSegmentReferences = append(largeSegmentReferences, fetchResult.largeSegmentReferences...)
		updatedSplitNames = append(updatedSplitNames, fetchResult.updatedSplitNames...)
		if fetchResult.ffCurrentSince == fetchResult.ffSince && fetchResult.rbCurrentSince == fetchResult.rbSince {
			s.runtimeTelemetry.RecordSuccessfulSync(telemetry.SplitSync, time.Now().UTC())
			break
		}
	}
	return &UpdateResult{
		UpdatedSplits:           common.DedupeStringSlice(updatedSplitNames),
		ReferencedSegments:      common.DedupeStringSlice(segmentReferences),
		NewChangeNumber:         fetchResult.ffCurrentSince,
		NewRBChangeNumber:       fetchResult.rbCurrentSince,
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
			s.validator.ProcessMatchers(&splitChanges.FeatureFlags.Splits[idx], s.logger)
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

func (s *UpdaterImpl) getSegments(ruleBasedSegment *dtos.RuleBasedSegmentDTO) []string {
	seen := make(map[string]struct{})
	segments := make([]string, 0)

	for _, segment := range ruleBasedSegment.Excluded.Segments {
		if segment.Type == TypeStandard {
			segments = addIfNotExists(segments, seen, segment.Name)
		}
	}

	for _, cond := range ruleBasedSegment.Conditions {
		for _, matcher := range cond.MatcherGroup.Matchers {
			if matcher.MatcherType == matcherTypeInSegment && matcher.UserDefinedSegment != nil {
				segments = addIfNotExists(segments, seen, matcher.UserDefinedSegment.SegmentName)
			}
		}
	}

	return segments
}

func (s *UpdaterImpl) processRuleBasedSegmentChanges(splitChanges *dtos.SplitChangesDTO) ([]dtos.RuleBasedSegmentDTO, []dtos.RuleBasedSegmentDTO, []string) {
	toRemove := make([]dtos.RuleBasedSegmentDTO, 0, len(splitChanges.RuleBasedSegments.RuleBasedSegments))
	toAdd := make([]dtos.RuleBasedSegmentDTO, 0, len(splitChanges.RuleBasedSegments.RuleBasedSegments))
	segments := make([]string, 0)
	for _, rbSegment := range splitChanges.RuleBasedSegments.RuleBasedSegments {
		if rbSegment.Status == Active {
			s.validator.ProcessRBMatchers(&rbSegment, s.logger)
			toAdd = append(toAdd, rbSegment)
			segments = append(segments, s.getSegments(&rbSegment)...)
		} else {
			toRemove = append(toRemove, rbSegment)
		}
	}
	return toAdd, toRemove, segments
}

func (s *UpdaterImpl) processRuleBasedChangeUpdate(ruleBasedChange dtos.SplitChangeUpdate) *UpdateResult {
	changeNumber := s.ruleBasedSegmentStorage.ChangeNumber()
	if changeNumber >= ruleBasedChange.BaseUpdate.ChangeNumber() {
		s.logger.Debug("the rule-based segment it's already updated")
		return &UpdateResult{RequiresFetch: true}
	}
	ruleBasedSegments := make([]dtos.RuleBasedSegmentDTO, 0, 1)
	ruleBasedSegments = append(ruleBasedSegments, *ruleBasedChange.RuleBasedSegment())
	splitChanges := dtos.SplitChangesDTO{RuleBasedSegments: dtos.RuleBasedSegmentsDTO{RuleBasedSegments: ruleBasedSegments}}
	toRemove, toAdd, segments := s.processRuleBasedSegmentChanges(&splitChanges)
	s.ruleBasedSegmentStorage.Update(toAdd, toRemove, ruleBasedChange.BaseUpdate.ChangeNumber())

	return &UpdateResult{
		ReferencedSegments: segments,
		NewRBChangeNumber:  ruleBasedChange.BaseUpdate.ChangeNumber(),
		RequiresFetch:      false,
	}
}

func (s *UpdaterImpl) SynchronizeFeatureFlags(ffChange *dtos.SplitChangeUpdate) (*UpdateResult, error) {
	var result *UpdateResult
	if ffChange.UpdateType() == UpdateTypeSplitChange {
		result = s.processFFChange(*ffChange)
	} else {
		result = s.processRuleBasedChangeUpdate(*ffChange)
	}
	if result.RequiresFetch {
		return s.SynchronizeSplits(common.Int64Ref(ffChange.BaseUpdate.ChangeNumber()))
	}
	return result, nil
}
