package grammar

import (
	"testing"

	"github.com/splitio/go-split-commons/v8/dtos"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/stretchr/testify/assert"
)

func TestBuildMatcher_InRuleBasedSegment(t *testing.T) {
	tests := []struct {
		name        string
		dto         *dtos.MatcherDTO
		wantErr     bool
		errContains string
	}{
		{
			name: "valid rule-based segment matcher",
			dto: &dtos.MatcherDTO{
				MatcherType: MatcherTypeInRuleBasedSegment,
				Negate:      false,
				UserDefinedSegment: &dtos.UserDefinedSegmentMatcherDataDTO{
					SegmentName: "segment1",
				},
			},
			wantErr: false,
		},
		{
			name: "missing UserDefinedSegment",
			dto: &dtos.MatcherDTO{
				MatcherType: MatcherTypeInRuleBasedSegment,
				Negate:      false,
			},
			wantErr:     true,
			errContains: "UserDefinedSegment is required for IN_RULE_BASED_SEGMENT matcher type",
		},
		{
			name: "with attribute name",
			dto: &dtos.MatcherDTO{
				MatcherType: MatcherTypeInRuleBasedSegment,
				Negate:      true,
				UserDefinedSegment: &dtos.UserDefinedSegmentMatcherDataDTO{
					SegmentName: "segment1",
				},
				KeySelector: &dtos.KeySelectorDTO{
					Attribute: stringPtr("attr1"),
				},
			},
			wantErr: false,
		},
	}

	logger := logging.NewLogger(&logging.LoggerOptions{})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			ruleBuilder := NewRuleBuilder(nil, nil, nil, SyncProxyFeatureFlagsRules, SyncProxyRuleBasedSegmentRules, logger, nil)

			matcher, err := ruleBuilder.BuildMatcher(tt.dto)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, matcher)

			// Type assertions
			ruleBasedSegmentMatcher, ok := matcher.(*InRuleBasedSegmentMatcher)
			assert.True(t, ok, "matcher should be of type *InRuleBasedSegmentMatcher")

			// Check fields
			assert.Equal(t, tt.dto.Negate, ruleBasedSegmentMatcher.negate)
			assert.Equal(t, tt.dto.UserDefinedSegment.SegmentName, ruleBasedSegmentMatcher.name)

			if tt.dto.KeySelector != nil && tt.dto.KeySelector.Attribute != nil {
				assert.Equal(t, tt.dto.KeySelector.Attribute, ruleBasedSegmentMatcher.attributeName)
			} else {
				assert.Nil(t, ruleBasedSegmentMatcher.attributeName)
			}

			assert.NotNil(t, ruleBasedSegmentMatcher.logger)
		})
	}
}

func stringPtr(s string) *string {
	return &s
}
