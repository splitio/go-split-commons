package split

import (
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
	ErrProxy = fmt.Errorf("version not supported in proxy")
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
	isProxy                     bool
	lastSyncNewSpec             int64
	specVersion                 string
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
		validator:                   validator.NewValidator(ruleBuilder),
		isProxy:                     isProxy,
		lastSyncNewSpec:             0,
		specVersion:                 specs.FLAG_V1_3,
	}
}

func (s *UpdaterImpl) SetRuleBasedSegmentStorage(storage storage.RuleBasedSegmentsStorage) {
	s.ruleBasedSegmentStorage = storage
}

func (s *UpdaterImpl) processUpdate(splitChanges dtos.FFResponse) {
	activeSplits, inactiveSplits := s.processFeatureFlagChanges(splitChanges.FeatureFlags())
	// Add/Update active splits
	s.splitStorage.Update(activeSplits, inactiveSplits, splitChanges.FFTill())
}

func (s *UpdaterImpl) processRuleBasedUpdate(splitChanges dtos.FFResponse) []string {
	activeRB, inactiveRB, segments := s.processRuleBasedSegmentChanges(splitChanges.RuleBasedSegments())
	// Add/Update active splits
	s.ruleBasedSegmentStorage.Update(activeRB, inactiveRB, splitChanges.RBTill())
	return segments
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
		var splitChanges dtos.FFResponse
		splitChanges, err = s.splitFetcher.Fetch(fetchOptions.WithChangeNumber(currentSince).WithChangeNumberRB(currentRBSince).WithSpecVersion(common.StringRef(s.specVersion)))
		if err != nil {
			if httpError, ok := err.(*dtos.HTTPError); ok {
				if httpError.Code == http.StatusRequestURITooLong {
					s.logger.Error("SDK Initialization, the amount of flag sets provided are big causing uri length error.")
				}
				s.runtimeTelemetry.RecordSyncError(telemetry.SplitSync, httpError.Code)
			}
			break
		}
		currentSince = splitChanges.FFTill()
		currentRBSince = splitChanges.RBTill()
		s.runtimeTelemetry.RecordSyncLatency(telemetry.SplitSync, time.Since(before))
		s.processUpdate(splitChanges)
		segmentReferences = s.processRuleBasedUpdate(splitChanges)
		segmentReferences = appendSegmentNames(segmentReferences, splitChanges.FeatureFlags())
		updatedSplitNames = appendSplitNames(updatedSplitNames, splitChanges.FeatureFlags())
		largeSegmentReferences = appendLargeSegmentNames(largeSegmentReferences, splitChanges.FeatureFlags())
		if splitChanges.NeedsAnotherFetch() {
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

func (s *UpdaterImpl) synchronizeSplits(till *int64) (*UpdateResult, error) {
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

func (s *UpdaterImpl) attemptLatestSync() (*UpdateResult, error) {
	s.hcMonitor.NotifyEvent(application.Splits)
	// just guessing sizes so the we don't realloc immediately
	updatedSplitNames := make([]string, 0, 10)
	largeSegmentReferences := make([]string, 0, 10)
	currentSince := int64(-1)
	currentRBSince := int64(-1)

	before := time.Now()
	splitChanges, err := s.splitFetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(currentSince).WithChangeNumberRB(currentRBSince).WithSpecVersion(common.StringRef(specs.FLAG_V1_3)))
	if err != nil {
		if httpError, ok := err.(*dtos.HTTPError); ok {
			if httpError.Code == http.StatusRequestURITooLong {
				s.logger.Error("SDK Initialization, the amount of flag sets provided are big causing uri length error.")
			}
			if httpError.Code == http.StatusBadRequest {
				return nil, ErrProxy
			}
			s.runtimeTelemetry.RecordSyncError(telemetry.SplitSync, httpError.Code)
		}
	}
	currentSince = splitChanges.FFTill()
	currentRBSince = splitChanges.RBTill()
	s.runtimeTelemetry.RecordSyncLatency(telemetry.SplitSync, time.Since(before))
	s.splitStorage.ReplaceAll(splitChanges.FeatureFlags(), currentSince)
	s.ruleBasedSegmentStorage.Clear()
	segmentReferences := s.processRuleBasedUpdate(splitChanges) // TODO ReplaceAll
	segmentReferences = appendSegmentNames(segmentReferences, splitChanges.FeatureFlags())
	updatedSplitNames = appendSplitNames(updatedSplitNames, splitChanges.FeatureFlags())
	largeSegmentReferences = appendLargeSegmentNames(largeSegmentReferences, splitChanges.FeatureFlags())
	return &UpdateResult{
		UpdatedSplits:           common.DedupeStringSlice(updatedSplitNames),
		ReferencedSegments:      common.DedupeStringSlice(segmentReferences),
		NewChangeNumber:         currentSince,
		NewRBChangeNumber:       currentRBSince,
		ReferencedLargeSegments: common.DedupeStringSlice(largeSegmentReferences),
	}, err
}

func (s *UpdaterImpl) passedTimeWindow() bool {
	return s.lastSyncNewSpec == 0 || (s.lastSyncNewSpec > 0 && time.Since(time.Unix(s.lastSyncNewSpec, 0)) >= 24*time.Hour)
}

func (s *UpdaterImpl) SynchronizeSplits(till *int64) (*UpdateResult, error) {
	if !s.isProxy && till != nil {
		return s.synchronizeSplits(till)
	}

	if s.isProxy && s.passedTimeWindow() {
		s.logger.Info("Attempting to sync splits with the latest spec version (v1.3)")
		syncResult, err := s.attemptLatestSync()
		s.lastSyncNewSpec = time.Now().Unix()
		if err == nil {
			s.lastSyncNewSpec = -1
			s.specVersion = specs.FLAG_V1_3
			untilTillSince, err := s.synchronizeSplits(nil)
			if err == nil {
				return &UpdateResult{
					UpdatedSplits:           common.DedupeStringSlice(append(syncResult.UpdatedSplits, untilTillSince.UpdatedSplits...)),
					ReferencedSegments:      common.DedupeStringSlice(append(syncResult.ReferencedSegments, untilTillSince.ReferencedSegments...)),
					NewChangeNumber:         untilTillSince.NewChangeNumber,
					NewRBChangeNumber:       untilTillSince.NewRBChangeNumber,
					ReferencedLargeSegments: common.DedupeStringSlice(append(syncResult.ReferencedLargeSegments, untilTillSince.ReferencedLargeSegments...)),
				}, nil
			}
		}
		s.specVersion = specs.FLAG_V1_1
	}

	return s.synchronizeSplits(nil)
}

func appendSplitNames(dst []string, featureFlags []dtos.SplitDTO) []string {
	for idx := range featureFlags {
		dst = append(dst, featureFlags[idx].Name)
	}
	return dst
}

func appendSegmentNames(dst []string, featureFlags []dtos.SplitDTO) []string {
	for _, split := range featureFlags {
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

func appendLargeSegmentNames(dst []string, featureFlags []dtos.SplitDTO) []string {
	for _, split := range featureFlags {
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

func appendRuleBasedSegmentNames(dst []string, featureFlags []dtos.SplitDTO) []string {
	seen := make(map[string]struct{})
	// Inicializamos el mapa con lo que ya tiene dst para no duplicar tampoco ahí
	for _, name := range dst {
		seen[name] = struct{}{}
	}

	for _, split := range featureFlags {
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

func (s *UpdaterImpl) processFeatureFlagChanges(featureFlags []dtos.SplitDTO) ([]dtos.SplitDTO, []dtos.SplitDTO) {
	toRemove := make([]dtos.SplitDTO, 0, len(featureFlags))
	toAdd := make([]dtos.SplitDTO, 0, len(featureFlags))

	for idx := range featureFlags {
		if featureFlags[idx].Status == Active && s.flagSetsFilter.Instersect(featureFlags[idx].Sets) {
			s.validator.ProcessMatchers(&featureFlags[idx], s.logger)
			toAdd = append(toAdd, featureFlags[idx])
		} else {
			toRemove = append(toRemove, featureFlags[idx])
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
	activeFFs, inactiveFFs := s.processFeatureFlagChanges(featureFlags)
	s.splitStorage.Update(activeFFs, inactiveFFs, ffChange.BaseUpdate.ChangeNumber())
	s.runtimeTelemetry.RecordUpdatesFromSSE(telemetry.SplitUpdate)
	updatedSplitNames = append(updatedSplitNames, ffChange.FeatureFlag().Name)
	segmentReferences = appendSegmentNames(segmentReferences, featureFlags)
	largeSegmentReferences = appendLargeSegmentNames(largeSegmentReferences, featureFlags)
	ruleBasedSegmentReferences = appendRuleBasedSegmentNames(ruleBasedSegmentReferences, featureFlags)
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

func (s *UpdaterImpl) processRuleBasedSegmentChanges(ruleBasedSegments []dtos.RuleBasedSegmentDTO) ([]dtos.RuleBasedSegmentDTO, []dtos.RuleBasedSegmentDTO, []string) {
	toRemove := make([]dtos.RuleBasedSegmentDTO, 0, len(ruleBasedSegments))
	toAdd := make([]dtos.RuleBasedSegmentDTO, 0, len(ruleBasedSegments))
	segments := make([]string, 0)
	for _, rbSegment := range ruleBasedSegments {
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
	toRemove, toAdd, segments := s.processRuleBasedSegmentChanges(ruleBasedSegments)
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
