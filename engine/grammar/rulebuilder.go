package grammar

import (
	"errors"
	"fmt"

	"golang.org/x/exp/slices"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/engine/grammar/datatypes"
	"github.com/splitio/go-split-commons/v6/storage"
	"github.com/splitio/go-toolkit/v5/logging"
)

type dependencyEvaluator interface {
	EvaluateDependency(key string, bucketingKey *string, feature string, attributes map[string]interface{}) string
}

type RuleBuilder struct {
	segmentStorage          storage.SegmentStorageConsumer
	ruleBasedSegmentStorage storage.RuleBasedSegmentStorageConsumer
	largeSegmentStorage     storage.LargeSegmentStorageConsumer
	ffAcceptedMatchers      []string
	rbAcceptedMatchers      []string
	logger                  logging.LoggerInterface
	dependencyEvaluator     dependencyEvaluator
}

func NewRuleBuilder(segmentStorage storage.SegmentStorageConsumer, ruleBasedSegmentStorage storage.RuleBasedSegmentStorageConsumer, largeSegmentStorage storage.LargeSegmentStorageConsumer, ffAcceptedMatchers []string, rbAcceptedMatchers []string, logger logging.LoggerInterface, dedependencyEvaluator dependencyEvaluator) RuleBuilder {
	return RuleBuilder{
		segmentStorage:          segmentStorage,
		ruleBasedSegmentStorage: ruleBasedSegmentStorage,
		largeSegmentStorage:     largeSegmentStorage,
		ffAcceptedMatchers:      ffAcceptedMatchers,
		rbAcceptedMatchers:      rbAcceptedMatchers,
		logger:                  logger,
		dependencyEvaluator:     dedependencyEvaluator,
	}
}

func (r RuleBuilder) BuildMatcher(dto *dtos.MatcherDTO) (MatcherInterface, error) {
	if !slices.Contains(r.ffAcceptedMatchers, dto.MatcherType) {
		return nil, datatypes.UnsupportedMatcherError{
			Message: fmt.Sprintf("Unable to create matcher for matcher type: %s", dto.MatcherType),
		}
	}

	var matcher MatcherInterface

	var attributeName *string
	if dto.KeySelector != nil {
		attributeName = dto.KeySelector.Attribute
	}

	switch dto.MatcherType {
	case MatcherTypeAllKeys:
		r.logger.Debug(fmt.Sprintf("Building AllKeysMatcher with negate=%t", dto.Negate))
		matcher = NewAllKeysMatcher(dto.Negate)

	case MatcherTypeEqualTo:
		if dto.UnaryNumeric == nil {
			return nil, errors.New("UnaryNumeric is required for EQUAL_TO matcher type")
		}
		r.logger.Debug(fmt.Sprintf(
			"Building EqualToMatcher with negate=%t, value=%d, type=%s, attributeName=%v",
			dto.Negate, dto.UnaryNumeric.Value, dto.UnaryNumeric.DataType, attributeName,
		))
		matcher = NewEqualToMatcher(
			dto.Negate,
			dto.UnaryNumeric.Value,
			dto.UnaryNumeric.DataType,
			attributeName,
		)

	case MatcherTypeInSegment:
		if dto.UserDefinedSegment == nil {
			return nil, errors.New("UserDefinedSegment is required for IN_SEGMENT matcher type")
		}
		r.logger.Debug(fmt.Sprintf(
			"Building InSegmentMatcher with negate=%t, segmentName=%s, attributeName=%v",
			dto.Negate, dto.UserDefinedSegment.SegmentName, attributeName,
		))
		matcher = NewInSegmentMatcher(
			dto.Negate,
			dto.UserDefinedSegment.SegmentName,
			attributeName,
			r.segmentStorage,
		)

	case MatcherTypeWhitelist:
		if dto.Whitelist == nil {
			return nil, errors.New("Whitelist is required for WHITELIST matcher type")
		}
		r.logger.Debug(fmt.Sprintf(
			"Building WhitelistMatcher with negate=%t, whitelist=%v, attributeName=%v",
			dto.Negate, dto.Whitelist.Whitelist, attributeName,
		))
		matcher = NewWhitelistMatcher(
			dto.Negate,
			dto.Whitelist.Whitelist,
			attributeName,
		)

	case MatcherTypeGreaterThanOrEqualTo:
		if dto.UnaryNumeric == nil {
			return nil, errors.New("UnaryNumeric is required for GREATER_THAN_OR_EQUAL_TO matcher type")
		}
		r.logger.Debug(fmt.Sprintf(
			"Building GreaterThanOrEqualToMatcher with negate=%t, value=%d, type=%s, attributeName=%v",
			dto.Negate, dto.UnaryNumeric.Value, dto.UnaryNumeric.DataType, attributeName,
		))
		matcher = NewGreaterThanOrEqualToMatcher(
			dto.Negate,
			dto.UnaryNumeric.Value,
			dto.UnaryNumeric.DataType,
			attributeName,
		)

	case MatcherTypeLessThanOrEqualTo:
		if dto.UnaryNumeric == nil {
			return nil, errors.New("UnaryNumeric is required for LESS_THAN_OR_EQUAL_TO matcher type")
		}
		r.logger.Debug(fmt.Sprintf(
			"Building LessThanOrEqualToMatcher with negate=%t, value=%d, type=%s, attributeName=%v",
			dto.Negate, dto.UnaryNumeric.Value, dto.UnaryNumeric.DataType, attributeName,
		))
		matcher = NewLessThanOrEqualToMatcher(
			dto.Negate,
			dto.UnaryNumeric.Value,
			dto.UnaryNumeric.DataType,
			attributeName,
		)

	case MatcherTypeBetween:
		if dto.Between == nil {
			return nil, errors.New("Between is required for BETWEEN matcher type")
		}
		r.logger.Debug(fmt.Sprintf(
			"Building BetweenMatcher with negate=%t, start=%d, end=%d, type=%s, attributeName=%v",
			dto.Negate, dto.Between.Start, dto.Between.End, dto.Between.DataType, attributeName,
		))
		matcher = NewBetweenMatcher(
			dto.Negate,
			dto.Between.Start,
			dto.Between.End,
			dto.Between.DataType,
			attributeName,
		)

	case MatcherTypeEqualToSet:
		if dto.Whitelist == nil {
			return nil, errors.New("Whitelist is required for EQUAL_TO_SET matcher type")
		}
		r.logger.Debug(fmt.Sprintf(
			"Building EqualToSetMatcher with negate=%t, set=%v, attributeName=%v",
			dto.Negate, dto.Whitelist.Whitelist, attributeName,
		))
		matcher = NewEqualToSetMatcher(
			dto.Negate,
			dto.Whitelist.Whitelist,
			attributeName,
		)

	case MatcherTypePartOfSet:
		if dto.Whitelist == nil {
			return nil, errors.New("Whitelist is required for PART_OF_SET matcher type")
		}
		r.logger.Debug(fmt.Sprintf(
			"Building PartOfSetMatcher with negate=%t, set=%v, attributeName=%v",
			dto.Negate, dto.Whitelist.Whitelist, attributeName,
		))
		matcher = NewPartOfSetMatcher(
			dto.Negate,
			dto.Whitelist.Whitelist,
			attributeName,
		)

	case MatcherTypeContainsAllOfSet:
		if dto.Whitelist == nil {
			return nil, errors.New("Whitelist is required for CONTAINS_ALL_OF_SET matcher type")
		}
		r.logger.Debug(fmt.Sprintf(
			"Building AllOfSetMatcher with negate=%t, set=%v, attributeName=%v",
			dto.Negate, dto.Whitelist.Whitelist, attributeName,
		))
		matcher = NewContainsAllOfSetMatcher(
			dto.Negate,
			dto.Whitelist.Whitelist,
			attributeName,
		)

	case MatcherTypeContainsAnyOfSet:
		if dto.Whitelist == nil {
			return nil, errors.New("Whitelist is required for CONTAINS_ANY_OF_SET matcher type")
		}
		r.logger.Debug(fmt.Sprintf(
			"Building AnyOfSetMatcher with negate=%t, set=%v, attributeName=%v",
			dto.Negate, dto.Whitelist.Whitelist, attributeName,
		))
		matcher = NewContainsAnyOfSetMatcher(
			dto.Negate,
			dto.Whitelist.Whitelist,
			attributeName,
		)

	case MatcherTypeStartsWith:
		if dto.Whitelist == nil {
			return nil, errors.New("Whitelist is required for STARTS_WITH matcher type")
		}
		r.logger.Debug(fmt.Sprintf(
			"Building StartsWithMatcher with negate=%t, set=%v, attributeName=%v",
			dto.Negate, dto.Whitelist.Whitelist, attributeName,
		))
		matcher = NewStartsWithMatcher(
			dto.Negate,
			dto.Whitelist.Whitelist,
			attributeName,
		)

	case MatcherTypeEndsWith:
		if dto.Whitelist == nil {
			return nil, errors.New("Whitelist is required for ENDS_WITH matcher type")
		}
		r.logger.Debug(fmt.Sprintf(
			"Building EndsWithMatcher with negate=%t, set=%v, attributeName=%v",
			dto.Negate, dto.Whitelist.Whitelist, attributeName,
		))
		matcher = NewEndsWithMatcher(
			dto.Negate,
			dto.Whitelist.Whitelist,
			attributeName,
		)

	case MatcherTypeContainsString:
		if dto.Whitelist == nil {
			return nil, errors.New("Whitelist is required for CONTAINS_STRING matcher type")
		}
		r.logger.Debug(fmt.Sprintf(
			"Building ContainsStringMatcher with negate=%t, set=%v, attributeName=%v",
			dto.Negate, dto.Whitelist.Whitelist, attributeName,
		))
		matcher = NewContainsStringMatcher(
			dto.Negate,
			dto.Whitelist.Whitelist,
			attributeName,
		)

	case MatcherTypeInSplitTreatment:
		if dto.Dependency == nil {
			return nil, errors.New("Dependency is required for IN_SPLIT_TREATMENT matcher type")
		}
		r.logger.Debug(fmt.Sprintf(
			"Building DependencyMatcher with negate=%t, feature=%s, treatments=%v, attributeName=%v",
			dto.Negate, dto.Dependency.Split, dto.Dependency.Treatments, attributeName,
		))
		matcher = NewDependencyMatcher(
			dto.Negate,
			dto.Dependency.Split,
			dto.Dependency.Treatments,
			r.dependencyEvaluator,
		)

	case MatcherTypeEqualToBoolean:
		if dto.Boolean == nil {
			return nil, errors.New("Boolean is required for EQUAL_TO_BOOLEAN matcher type")
		}
		r.logger.Debug(fmt.Sprintf(
			"Building BooleanMatcher with negate=%t, value=%t, attributeName=%v",
			dto.Negate, *dto.Boolean, attributeName,
		))
		matcher = NewBooleanMatcher(
			dto.Negate,
			dto.Boolean,
			attributeName,
		)

	case MatcherTypeMatchesString:
		if dto.String == nil {
			return nil, errors.New("String is required for MATCHES_STRING matcher type")
		}
		r.logger.Debug(fmt.Sprintf(
			"Building RegexMatcher with negate=%t, regex=%s, attributeName=%v",
			dto.Negate, *dto.String, attributeName,
		))
		matcher = NewRegexMatcher(
			dto.Negate,
			*dto.String,
			attributeName,
		)
	case MatcherEqualToSemver:
		if dto.String == nil {
			return nil, ErrInvalidEqualSemver
		}
		r.logger.Debug(fmt.Sprintf(
			"Building EqualToSemverMatcher with negate=%t, regex=%s, attributeName=%v",
			dto.Negate, *dto.String, attributeName,
		))
		matcher = NewEqualToSemverMatcher(
			*dto.String,
			dto.Negate,
			attributeName,
			r.logger,
		)
	case MatcherTypeGreaterThanOrEqualToSemver:
		if dto.String == nil {
			return nil, ErrInvalidGTOESemver
		}
		r.logger.Debug(fmt.Sprintf(
			"Building GreaterThanOrEqualToSemverMatcher with negate=%t, semver=%s, attributeName=%v",
			dto.Negate, *dto.String, attributeName,
		))
		matcher = NewGreaterThanOrEqualToSemverMatcher(
			dto.Negate,
			*dto.String,
			attributeName,
			r.logger,
		)
	case MatcherTypeLessThanOrEqualToSemver:
		if dto.String == nil {
			return nil, ErrInvalidLTOESemver
		}
		r.logger.Debug(fmt.Sprintf(
			"Building LessThanOrEqualToSemverMatcher with negate=%t, regex=%s, attributeName=%v",
			dto.Negate, *dto.String, attributeName,
		))
		matcher = NewLessThanOrEqualToSemverMatcher(
			*dto.String,
			dto.Negate,
			attributeName,
			r.logger,
		)
	case MatcherTypeBetweenSemver:
		if dto.BetweenString.Start == nil || dto.BetweenString.End == nil {
			return nil, ErrInvalidLBetweenSemver
		}
		r.logger.Debug(fmt.Sprintf(
			"Building BetweenSemverMatcher with negate=%t, regexStart=%s, regexEnd=%s, attributeName=%v",
			dto.Negate, *dto.BetweenString.Start, *dto.BetweenString.End, attributeName,
		))
		matcher = NewBetweenSemverMatcher(
			*dto.BetweenString.Start,
			*dto.BetweenString.End,
			dto.Negate,
			attributeName,
			r.logger,
		)
	case MatcherTypeInListSemver:
		if dto.Whitelist == nil {
			return nil, ErrInvalidLInListSemver
		}
		r.logger.Debug(fmt.Sprintf(
			"Building ErrInvalidLInListSemver with negate=%t, regex=%v, attributeName=%v",
			dto.Negate, dto.Whitelist.Whitelist, attributeName,
		))
		matcher = NewInListSemverMatcher(
			dto.Whitelist.Whitelist,
			dto.Negate,
			attributeName,
			r.logger,
		)
	case MatcherTypeInLargeSegment:
		if dto.UserDefinedLargeSegment == nil {
			return nil, errors.New("UserDefinedLargeSegment is required for IN_LARGE_SEGMENT matcher type")
		}
		r.logger.Debug(fmt.Sprintf(
			"Building InLargeSegmentMatcher with negate=%t, largeSegmentName=%s, attributeName=%v",
			dto.Negate, dto.UserDefinedLargeSegment.LargeSegmentName, attributeName,
		))
		matcher = NewInLargeSegmentMatcher(
			dto.Negate,
			dto.UserDefinedLargeSegment.LargeSegmentName,
			attributeName,
			r.largeSegmentStorage,
		)
	case MatcherTypeInRuleBasedSegment:
		if dto.UserDefinedSegment == nil {
			return nil, errors.New("UserDefinedSegment is required for IN_RULE_BASED_SEGMENT matcher type")
		}
		r.logger.Debug(fmt.Sprintf(
			"Building InRuleBasedSegmentMatcher with negate=%t, ruleBasedSegmentName=%s, attributeName=%v",
			dto.Negate, dto.UserDefinedSegment.SegmentName, attributeName,
		))
		matcher = NewInRuleBasedSegmentMatcher(
			dto.Negate,
			dto.UserDefinedSegment.SegmentName,
			attributeName,
			r,
		)
	default:
		return nil, datatypes.UnsupportedMatcherError{
			Message: fmt.Sprintf("Unable to create matcher for matcher type: %s", dto.MatcherType),
		}
	}

	matcher.base().logger = r.logger

	return matcher, nil
}
