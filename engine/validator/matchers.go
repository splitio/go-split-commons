package validator

import (
	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-split-commons/v5/engine/evaluator"
	"github.com/splitio/go-split-commons/v5/engine/evaluator/impressionlabels"
	"github.com/splitio/go-split-commons/v5/engine/grammar"
	"github.com/splitio/go-split-commons/v5/engine/grammar/matchers"
	"github.com/splitio/go-toolkit/v5/injection"
	"github.com/splitio/go-toolkit/v5/logging"
)

// OverrideWithUnsupported overrides the split with an unsupported matcher type
func OverrideWithUnsupported(split *dtos.SplitDTO, idx int, jdx int) {
	split.Conditions[idx].ConditionType = grammar.ConditionTypeWhitelist
	split.Conditions[idx].MatcherGroup.Matchers[jdx].MatcherType = matchers.MatcherTypeAllKeys
	split.Conditions[idx].MatcherGroup.Matchers[jdx].String = nil
	split.Conditions[idx].Label = impressionlabels.UnsupportedMatcherType
	split.Conditions[idx].Partitions = []dtos.PartitionDTO{{Treatment: evaluator.Control, Size: 100}}
}

// ProcessMatchers processes the matchers of a split and validates them
func ProcessMatchers(split *dtos.SplitDTO, logger logging.LoggerInterface) {
	for idx := range split.Conditions {
		for jdx := range split.Conditions[idx].MatcherGroup.Matchers {
			_, err := matchers.BuildMatcher(&split.Conditions[idx].MatcherGroup.Matchers[jdx], &injection.Context{}, logger)
			if err != nil {
				OverrideWithUnsupported(split, idx, jdx)
			}
		}
	}
}
