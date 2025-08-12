package rulebasedsegment

import (
	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/engine/validator"
	"github.com/splitio/go-split-commons/v6/storage"
	"github.com/splitio/go-toolkit/v3/logging"
)

const (
	Active               = "ACTIVE"
	Archived             = "ARCHIVED"
	TypeStandard         = "standard"
	TypeRuleBased        = "rule-based"
	TypeLarge            = "large"
	matcherTypeInSegment = "IN_SEGMENT"
)

// Updater interface
type Updater interface {
	SynchronizeRuleBasedSegment(rbChange *dtos.RuleBasedChangeUpdate) (*UpdateResult, error)
}

// UpdateResult encapsulates information regarding the split update performed
type UpdateResult struct {
	ReferencedSegments []string
	NewChangeNumber    int64
	RequiresFetch      bool
}

// UpdaterImpl struct for split sync
type UpdaterImpl struct {
	ruleBasedSegmentStorage storage.RuleBasedSegmentsStorage
	logger                  logging.LoggerInterface
}

// NewRuleBasedUpdater creates new split synchronizer for processing rule-based updates
func NewRuleBasedSegmentUpdater(
	ruleBasedSegmentStorage storage.RuleBasedSegmentsStorage,
	logger logging.LoggerInterface,
) *UpdaterImpl {
	return &UpdaterImpl{
		ruleBasedSegmentStorage: ruleBasedSegmentStorage,
		logger:                  logger,
	}
}

func (s *UpdaterImpl) SynchronizeRuleBasedSegment(ruleBasedChange *dtos.RuleBasedChangeUpdate) (*UpdateResult, error) {
	result := s.processRuleBasedChangeUpdate(ruleBasedChange)
	return result, nil
}

func (s *UpdaterImpl) ProcessUpdate(splitChanges *dtos.SplitChangesDTO) []string {
	activeRB, inactiveRB, segments := s.processRuleBasedSegmentChanges(splitChanges)
	// Add/Update active splits
	s.ruleBasedSegmentStorage.Update(activeRB, inactiveRB, splitChanges.RuleBasedSegments.Till)
	return segments
}

func (s *UpdaterImpl) processRuleBasedSegmentChanges(splitChanges *dtos.SplitChangesDTO) ([]dtos.RuleBasedSegmentDTO, []dtos.RuleBasedSegmentDTO, []string) {
	toRemove := make([]dtos.RuleBasedSegmentDTO, 0, len(splitChanges.RuleBasedSegments.RuleBasedSegments))
	toAdd := make([]dtos.RuleBasedSegmentDTO, 0, len(splitChanges.RuleBasedSegments.RuleBasedSegments))
	segments := make([]string, 0)
	for _, rbSegment := range splitChanges.RuleBasedSegments.RuleBasedSegments {
		if rbSegment.Status == Active {
			validator.ProcessRBMatchers(&rbSegment, s.logger)
			toAdd = append(toAdd, rbSegment)
			segments = append(segments, s.getSegments(&rbSegment)...)
		} else {
			toRemove = append(toRemove, rbSegment)
		}
	}
	return toAdd, toRemove, segments
}

func addIfNotExists(segments []string, seen map[string]struct{}, name string) []string {
	if _, exists := seen[name]; !exists {
		seen[name] = struct{}{}
		segments = append(segments, name)
	}
	return segments
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

func (s *UpdaterImpl) processRuleBasedChangeUpdate(ruleBasedChange *dtos.RuleBasedChangeUpdate) *UpdateResult {
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
		NewChangeNumber:    ruleBasedChange.BaseUpdate.ChangeNumber(),
		RequiresFetch:      false,
	}
}
