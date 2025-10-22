package dtos

import (
	"encoding/json"
)

// FFResponse is an interface that abstracts the differences between different versions of feature flag responses.
type FFResponse interface {
	NeedsAnotherFetch() bool
	RuleBasedSegments() []RuleBasedSegmentDTO
	FeatureFlags() []SplitDTO
	FFTill() int64
	RBTill() int64
	FFSince() int64
	RBSince() int64
}

// FFResponseLegacy handles the legacy format of feature flag responses.
type FFResponseLegacy struct {
	SplitChanges SplitChangesDTO
}

// NewFFResponseLegacy creates a new FFResponseLegacy instance from the provided JSON data.
func NewFFResponseLegacy(data []byte) (FFResponse, error) {
	var splitChangesDto SplitChangesDTO
	err := json.Unmarshal(data, &splitChangesDto)
	if err == nil {
		return &FFResponseLegacy{
			SplitChanges: splitChangesDto,
		}, nil
	}
	return nil, err
}

// NeedsAnotherFetch checks if another fetch is needed based on the since and till values.
func (ffLegacy *FFResponseLegacy) NeedsAnotherFetch() bool {
	return ffLegacy.SplitChanges.Since == ffLegacy.SplitChanges.Till
}

// FeatureFlags returns the list of feature flags (splits) from the response.
func (ffLegacy *FFResponseLegacy) FeatureFlags() []SplitDTO {
	return ffLegacy.SplitChanges.Splits
}

// RuleBasedSegments returns an empty list as legacy responses do not contain rule-based segments.
func (ffLegacy *FFResponseLegacy) RuleBasedSegments() []RuleBasedSegmentDTO {
	return []RuleBasedSegmentDTO{}
}

// FFTill returns the till value for feature flags.
func (ffLegacy *FFResponseLegacy) FFTill() int64 {
	return ffLegacy.SplitChanges.Till
}

// RBTill returns 0 as legacy responses do not contain rule-based segments.
func (ffLegacy *FFResponseLegacy) RBTill() int64 {
	return 0
}

// FFSince returns the since value for feature flags.
func (ffLegacy *FFResponseLegacy) FFSince() int64 {
	return ffLegacy.SplitChanges.Since
}

// RBSince returns 0 as legacy responses do not contain rule-based segments.
func (ffLegacy *FFResponseLegacy) RBSince() int64 {
	return 0
}

// FFResponseV13 handles the version 1.3 format of feature flag responses.
type FFResponseV13 struct {
	SplitChanges RuleChangesDTO
}

// NewFFResponseV13 creates a new FFResponseV13 instance from the provided JSON data.
func NewFFResponseV13(data []byte) (FFResponse, error) {
	var splitChangesDto RuleChangesDTO
	err := json.Unmarshal(data, &splitChangesDto)
	if err == nil {
		return &FFResponseV13{
			SplitChanges: splitChangesDto,
		}, nil
	}
	return nil, err
}

// FeatureFlags returns the list of feature flags (splits) from the response.
func (f13 *FFResponseV13) FeatureFlags() []SplitDTO {
	return f13.SplitChanges.FeatureFlags.Splits
}

// RuleBasedSegments returns the list of rule-based segments from the response.
func (f13 *FFResponseV13) RuleBasedSegments() []RuleBasedSegmentDTO {
	return f13.SplitChanges.RuleBasedSegments.RuleBasedSegments
}

// NeedsAnotherFetch checks if another fetch is needed based on the since and till values for both feature flags and rule-based segments.
func (f13 FFResponseV13) NeedsAnotherFetch() bool {
	return f13.SplitChanges.FeatureFlags.Since == f13.SplitChanges.FeatureFlags.Till && f13.SplitChanges.RuleBasedSegments.Since == f13.SplitChanges.RuleBasedSegments.Till
}

// FFTill returns the till value for feature flags.
func (f13 *FFResponseV13) FFTill() int64 {
	return f13.SplitChanges.FeatureFlags.Till
}

// RBTill returns the till value for rule-based segments.
func (f13 *FFResponseV13) RBTill() int64 {
	return f13.SplitChanges.RuleBasedSegments.Till
}

// RBSince returns the since value for rule-based segments.
func (f13 *FFResponseV13) RBSince() int64 {
	return f13.SplitChanges.RuleBasedSegments.Since
}

// FFSince returns the since value for feature flags.
func (f13 *FFResponseV13) FFSince() int64 {
	return f13.SplitChanges.FeatureFlags.Since
}

// FFResponseLocalV13 is a local version of FFResponseV13 for internal use.
type FFResponseLocalV13 struct {
	SplitChanges RuleChangesDTO
}

// NewFFResponseLocalV13 creates a new FFResponseLocalV13 instance from the provided JSON data.
func NewFFResponseLocalV13(data []byte) (*FFResponseLocalV13, error) {
	var splitChangesDto RuleChangesDTO
	err := json.Unmarshal(data, &splitChangesDto)
	if err == nil {
		return &FFResponseLocalV13{
			SplitChanges: splitChangesDto,
		}, nil
	}
	return nil, err
}

// FeatureFlags returns the list of feature flags (splits) from the response.
func (f *FFResponseLocalV13) FeatureFlags() []SplitDTO {
	return f.SplitChanges.FeatureFlags.Splits
}

// RuleBasedSegments returns the list of rule-based segments from the response.
func (f *FFResponseLocalV13) RuleBasedSegments() []RuleBasedSegmentDTO {
	return f.SplitChanges.RuleBasedSegments.RuleBasedSegments
}

// NeedsAnotherFetch checks if another fetch is needed based on the since and till values for both feature flags and rule-based segments.
func (f FFResponseLocalV13) NeedsAnotherFetch() bool {
	return f.SplitChanges.FeatureFlags.Since == f.SplitChanges.FeatureFlags.Till && f.SplitChanges.RuleBasedSegments.Since == f.SplitChanges.RuleBasedSegments.Till
}

// FFTill returns the till value for feature flags.
func (f *FFResponseLocalV13) FFTill() int64 {
	return f.SplitChanges.FeatureFlags.Till
}

// RBTill returns the till value for rule-based segments.
func (f *FFResponseLocalV13) RBTill() int64 {
	return f.SplitChanges.RuleBasedSegments.Till
}

// RBSince returns the since value for rule-based segments.
func (f *FFResponseLocalV13) RBSince() int64 {
	return f.SplitChanges.RuleBasedSegments.Since
}

// FFSince returns the since value for feature flags.
func (f *FFResponseLocalV13) SetFFTill(till int64) {
	f.SplitChanges.FeatureFlags.Till = till
}

// SetFFSince sets the since value for feature flags.
func (f *FFResponseLocalV13) SetFFSince(since int64) {
	f.SplitChanges.FeatureFlags.Since = since
}

// SetRBTill sets the till value for rule-based segments.
func (f *FFResponseLocalV13) SetRBTill(till int64) {
	f.SplitChanges.RuleBasedSegments.Till = till
}

// FFSince returns the since value for feature flags.
func (f *FFResponseLocalV13) FFSince() int64 {
	return f.SplitChanges.FeatureFlags.Since
}

// SetRBSince sets the since value for rule-based segments.
func (f *FFResponseLocalV13) SetRBSince(since int64) {
	f.SplitChanges.RuleBasedSegments.Since = since
}

// ReplaceFF replaces the feature flags (splits) in the response with the provided list.
func (f *FFResponseLocalV13) ReplaceFF(featureFlags []SplitDTO) {
	f.SplitChanges.FeatureFlags.Splits = featureFlags
}

// ReplaceRB replaces the rule-based segments in the response with the provided list.
func (f *FFResponseLocalV13) ReplaceRB(ruleBasedSegments []RuleBasedSegmentDTO) {
	f.SplitChanges.RuleBasedSegments.RuleBasedSegments = ruleBasedSegments
}

var _ FFResponse = (*FFResponseLegacy)(nil)
var _ FFResponse = (*FFResponseV13)(nil)
var _ FFResponse = (*FFResponseLocalV13)(nil)
