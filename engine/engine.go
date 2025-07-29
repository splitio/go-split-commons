package engine

import (
	"fmt"
	"math"

	"github.com/splitio/go-split-commons/v6/engine/evaluator/impressionlabels"
	"github.com/splitio/go-split-commons/v6/engine/grammar"
	"github.com/splitio/go-split-commons/v6/engine/grammar/constants"
	"github.com/splitio/go-split-commons/v6/engine/hash"

	"github.com/splitio/go-toolkit/v5/hasher"
	"github.com/splitio/go-toolkit/v5/logging"
)

// Engine struct is responsible for checking if any of the conditions of the feature flag matches,
// performing traffic allocation, calculating the bucket and returning the appropriate treatment
type Engine struct {
	logger logging.LoggerInterface
}

// DoEvaluation performs the main evaluation against each condition
func (e *Engine) DoEvaluation(
	split *grammar.Split,
	key string,
	bucketingKey string,
	attributes map[string]interface{},
) (*string, string) {
	inRollOut := false
	for _, cond := range split.Conditions() {
		if !inRollOut && cond.ConditionType() == grammar.ConditionTypeRollout {
			if split.TrafficAllocation() < 100 {
				bucket := e.calculateBucket(split.Algo(), bucketingKey, split.TrafficAllocationSeed())
				if bucket > split.TrafficAllocation() {
					e.logger.Debug(fmt.Sprintf(
						"Traffic allocation exceeded for feature %s and key %s."+
							" Returning default treatment", split.Name(), key,
					))
					defaultTreatment := split.DefaultTreatment()
					return &defaultTreatment, impressionlabels.NotInSplit
				}
				inRollOut = true
			}
		}

		if cond.Matches(key, &bucketingKey, attributes) {
			bucket := e.calculateBucket(split.Algo(), bucketingKey, split.Seed())
			treatment := cond.CalculateTreatment(bucket)
			return treatment, cond.Label()
		}
	}
	return nil, impressionlabels.NoConditionMatched
}

func (e *Engine) calculateBucket(algo int, bucketingKey string, seed int64) int {
	var hashedKey uint32
	switch algo {
	case constants.SplitAlgoMurmur:
		hashedKey = hasher.Sum32WithSeed([]byte(bucketingKey), uint32(seed))
	case constants.SplitAlgoLegacy:
		fallthrough
	default:
		hashedKey = hash.Legacy([]byte(bucketingKey), uint32(seed))
	}

	return int(math.Abs(float64(hashedKey%100)) + 1)

}

// NewEngine instantiates and returns a new engine
func NewEngine(logger logging.LoggerInterface) *Engine {
	return &Engine{logger: logger}
}
