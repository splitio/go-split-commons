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
	SetFFTill(till int64)
	SetFFSince(since int64)
	SetRBTill(till int64)
	SetRBSince(since int64)
	ReplaceFF(featureFlags []SplitDTO)
	ReplaceRB(ruleBasedSegments []RuleBasedSegmentDTO)
}

type FFResponseV12 struct {
	SplitChanges SplitsDTO
}

func NewFFResponseV12(data []byte) (FFResponse, error) {
	var splitChangesDto SplitsDTO
	err := json.Unmarshal(data, &splitChangesDto)
	if err == nil {
		return &FFResponseV12{
			SplitChanges: splitChangesDto,
		}, nil
	}
	return nil, err
}

func (f12 *FFResponseV12) NeedsAnotherFetch() bool {
	return f12.SplitChanges.Since == f12.SplitChanges.Till
}

func (f12 *FFResponseV12) FeatureFlags() []SplitDTO {
	return f12.SplitChanges.Splits
}

func (wf12 *FFResponseV12) RuleBasedSegments() []RuleBasedSegmentDTO {
	return []RuleBasedSegmentDTO{}
}

func (f12 *FFResponseV12) FFTill() int64 {
	return f12.SplitChanges.Till
}

func (f12 *FFResponseV12) RBTill() int64 {
	return 0
}

func (f12 *FFResponseV12) FFSince() int64 {
	return f12.FFSince()
}

func (f12 *FFResponseV12) RBSince() int64 {
	return 0
}

func (f12 *FFResponseV12) SetFFTill(till int64) {
	f12.SplitChanges.Till = till
}

func (f12 *FFResponseV12) SetFFSince(since int64) {
	f12.SplitChanges.Since = since
}

func (f12 *FFResponseV12) SetRBTill(till int64) {
	//no op
}

func (f12 *FFResponseV12) SetRBSince(since int64) {
	//no op
}

func (f12 *FFResponseV12) ReplaceFF(featureFlags []SplitDTO) {
	f12.SplitChanges.Splits = featureFlags
}

func (f12 *FFResponseV12) ReplaceRB(ruleBasedSegments []RuleBasedSegmentDTO) {
	//no op
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

func NewFFResponseWithFFRBV13(ffDTOs []SplitDTO, rbDTOs []RuleBasedSegmentDTO) FFResponse {
	featureFlagChange := SplitChangesDTO{FeatureFlags: FeatureFlagsDTO{Splits: ffDTOs},
		RuleBasedSegments: RuleBasedSegmentsDTO{RuleBasedSegments: rbDTOs}}
	return &FFResponseV13{
		SplitChanges: featureFlagChange,
	}
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

func (f13 *FFResponseV13) SetFFTill(till int64) {
	f13.SplitChanges.FeatureFlags.Till = till
}

func (f13 *FFResponseV13) SetFFSince(since int64) {
	f13.SplitChanges.FeatureFlags.Since = since
}

func (f13 *FFResponseV13) SetRBTill(till int64) {
	f13.SplitChanges.RuleBasedSegments.Till = till
}

func (f13 *FFResponseV13) FFSince() int64 {
	return f13.SplitChanges.FeatureFlags.Since
}

func (f13 *FFResponseV13) SetRBSince(since int64) {
	f13.SplitChanges.RuleBasedSegments.Since = since
}

func (f13 *FFResponseV13) ReplaceFF(featureFlags []SplitDTO) {
	f13.SplitChanges.FeatureFlags.Splits = featureFlags
}

func (f13 *FFResponseV13) ReplaceRB(ruleBasedSegments []RuleBasedSegmentDTO) {
	f13.SplitChanges.RuleBasedSegments.RuleBasedSegments = ruleBasedSegments
}
