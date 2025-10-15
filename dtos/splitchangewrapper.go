package dtos

import (
	"encoding/json"
)

type FFResponse interface {
	NeedsAnotherFetch() bool
	RuleBasedSegments() []RuleBasedSegmentDTO
	FeatureFlags() []SplitDTO
	FFTill() int64
	RBTill() int64
	FFSince() int64
	RBSince() int64
}

type FFResponseLegacy struct {
	SplitChanges SplitsDTO
}

func NewFFResponseLegacy(data []byte) (FFResponse, error) {
	var splitChangesDto SplitsDTO
	err := json.Unmarshal(data, &splitChangesDto)
	if err == nil {
		return &FFResponseLegacy{
			SplitChanges: splitChangesDto,
		}, nil
	}
	return nil, err
}

func (f12 *FFResponseLegacy) NeedsAnotherFetch() bool {
	return f12.SplitChanges.Since == f12.SplitChanges.Till
}

func (f12 *FFResponseLegacy) FeatureFlags() []SplitDTO {
	return f12.SplitChanges.Splits
}

func (wf12 *FFResponseLegacy) RuleBasedSegments() []RuleBasedSegmentDTO {
	return []RuleBasedSegmentDTO{}
}

func (f12 *FFResponseLegacy) FFTill() int64 {
	return f12.SplitChanges.Till
}

func (f12 *FFResponseLegacy) RBTill() int64 {
	return 0
}

func (f12 *FFResponseLegacy) FFSince() int64 {
	return f12.SplitChanges.Since
}

func (f12 *FFResponseLegacy) RBSince() int64 {
	return 0
}

type FFResponseV13 struct {
	SplitChanges SplitChangesDTO
}

func NewFFResponseV13(data []byte) (FFResponse, error) {
	var splitChangesDto SplitChangesDTO
	err := json.Unmarshal(data, &splitChangesDto)
	if err == nil {
		return &FFResponseV13{
			SplitChanges: splitChangesDto,
		}, nil
	}
	return nil, err
}

func (f13 *FFResponseV13) FeatureFlags() []SplitDTO {
	return f13.SplitChanges.FeatureFlags.Splits
}

func (f13 *FFResponseV13) RuleBasedSegments() []RuleBasedSegmentDTO {
	return f13.SplitChanges.RuleBasedSegments.RuleBasedSegments
}

func (f13 FFResponseV13) NeedsAnotherFetch() bool {
	return f13.SplitChanges.FeatureFlags.Since == f13.SplitChanges.FeatureFlags.Till && f13.SplitChanges.RuleBasedSegments.Since == f13.SplitChanges.RuleBasedSegments.Till
}

func (f13 *FFResponseV13) FFTill() int64 {
	return f13.SplitChanges.FeatureFlags.Till
}

func (f13 *FFResponseV13) RBTill() int64 {
	return f13.SplitChanges.RuleBasedSegments.Till
}

func (f13 *FFResponseV13) RBSince() int64 {
	return f13.SplitChanges.RuleBasedSegments.Since
}

func (f13 *FFResponseV13) FFSince() int64 {
	return f13.SplitChanges.FeatureFlags.Since
}

type FFResponseLocalV13 struct {
	SplitChanges SplitChangesDTO
}

func NewFFResponseLocalV13(data []byte) (*FFResponseLocalV13, error) {
	var splitChangesDto SplitChangesDTO
	err := json.Unmarshal(data, &splitChangesDto)
	if err == nil {
		return &FFResponseLocalV13{
			SplitChanges: splitChangesDto,
		}, nil
	}
	return nil, err
}

func (f *FFResponseLocalV13) FeatureFlags() []SplitDTO {
	return f.SplitChanges.FeatureFlags.Splits
}

func (f *FFResponseLocalV13) RuleBasedSegments() []RuleBasedSegmentDTO {
	return f.SplitChanges.RuleBasedSegments.RuleBasedSegments
}

func (f FFResponseLocalV13) NeedsAnotherFetch() bool {
	return f.SplitChanges.FeatureFlags.Since == f.SplitChanges.FeatureFlags.Till && f.SplitChanges.RuleBasedSegments.Since == f.SplitChanges.RuleBasedSegments.Till
}

func (f *FFResponseLocalV13) FFTill() int64 {
	return f.SplitChanges.FeatureFlags.Till
}

func (f *FFResponseLocalV13) RBTill() int64 {
	return f.SplitChanges.RuleBasedSegments.Till
}

func (f *FFResponseLocalV13) RBSince() int64 {
	return f.SplitChanges.RuleBasedSegments.Since
}

func (f *FFResponseLocalV13) SetFFTill(till int64) {
	f.SplitChanges.FeatureFlags.Till = till
}

func (f *FFResponseLocalV13) SetFFSince(since int64) {
	f.SplitChanges.FeatureFlags.Since = since
}

func (f *FFResponseLocalV13) SetRBTill(till int64) {
	f.SplitChanges.RuleBasedSegments.Till = till
}

func (f *FFResponseLocalV13) FFSince() int64 {
	return f.SplitChanges.FeatureFlags.Since
}

func (f *FFResponseLocalV13) SetRBSince(since int64) {
	f.SplitChanges.RuleBasedSegments.Since = since
}

func (f *FFResponseLocalV13) ReplaceFF(featureFlags []SplitDTO) {
	f.SplitChanges.FeatureFlags.Splits = featureFlags
}

func (f *FFResponseLocalV13) ReplaceRB(ruleBasedSegments []RuleBasedSegmentDTO) {
	f.SplitChanges.RuleBasedSegments.RuleBasedSegments = ruleBasedSegments
}
