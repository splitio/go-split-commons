package evaluator

import (
	"fmt"
	"time"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/engine"
	"github.com/splitio/go-split-commons/v6/engine/evaluator/impressionlabels"
	"github.com/splitio/go-split-commons/v6/engine/grammar"
	"github.com/splitio/go-split-commons/v6/storage"

	"github.com/splitio/go-toolkit/v5/injection"
	"github.com/splitio/go-toolkit/v5/logging"
)

const (
	// Control is the treatment returned when something goes wrong
	Control = "control"
)

// Result represents the result of an evaluation, including the resulting treatment, the label for the impression,
// the latency and error if any
type Result struct {
	Treatment           string
	Label               string
	EvaluationTime      time.Duration
	SplitChangeNumber   int64
	Config              *string
	ImpressionsDisabled bool
}

// Results represents the result of multiple evaluations at once
type Results struct {
	Evaluations    map[string]Result
	EvaluationTime time.Duration
}

// Evaluator struct is the main evaluator
type Evaluator struct {
	splitStorage   storage.SplitStorageConsumer
	segmentStorage storage.SegmentStorageConsumer
	eng            *engine.Engine
	logger         logging.LoggerInterface
}

// NewEvaluator instantiates an Evaluator struct and returns a reference to it
func NewEvaluator(
	splitStorage storage.SplitStorageConsumer,
	segmentStorage storage.SegmentStorageConsumer,
	eng *engine.Engine,
	logger logging.LoggerInterface,
) *Evaluator {
	return &Evaluator{
		splitStorage:   splitStorage,
		segmentStorage: segmentStorage,
		eng:            eng,
		logger:         logger,
	}
}

func (e *Evaluator) evaluateTreatment(key string, bucketingKey string, featureFlag string, splitDto *dtos.SplitDTO, attributes map[string]interface{}) *Result {
	var config *string
	if splitDto == nil {
		e.logger.Warning(fmt.Sprintf("Feature flag %s not found, returning control.", featureFlag))
		return &Result{Treatment: Control, Label: impressionlabels.SplitNotFound, Config: config}
	}

	ctx := injection.NewContext()
	ctx.AddDependency("segmentStorage", e.segmentStorage)
	ctx.AddDependency("evaluator", e)

	split := grammar.NewSplit(splitDto, ctx, e.logger)

	if split.Killed() {
		e.logger.Warning(fmt.Sprintf(
			"Feature flag %s has been killed, returning default treatment: %s",
			featureFlag,
			split.DefaultTreatment(),
		))

		if _, ok := split.Configurations()[split.DefaultTreatment()]; ok {
			treatmentConfig := split.Configurations()[split.DefaultTreatment()]
			config = &treatmentConfig
		}

		return &Result{
			Treatment:           split.DefaultTreatment(),
			Label:               impressionlabels.Killed,
			SplitChangeNumber:   split.ChangeNumber(),
			Config:              config,
			ImpressionsDisabled: split.ImpressionsDisabled(),
		}
	}

	treatment, label := e.eng.DoEvaluation(split, key, bucketingKey, attributes)
	if treatment == nil {
		e.logger.Warning(fmt.Sprintf(
			"No condition matched, returning default treatment: %s",
			split.DefaultTreatment(),
		))
		defaultTreatment := split.DefaultTreatment()
		treatment = &defaultTreatment
		label = impressionlabels.NoConditionMatched
	}

	if _, ok := split.Configurations()[*treatment]; ok {
		treatmentConfig := split.Configurations()[*treatment]
		config = &treatmentConfig
	}

	return &Result{
		Treatment:           *treatment,
		Label:               label,
		SplitChangeNumber:   split.ChangeNumber(),
		Config:              config,
		ImpressionsDisabled: split.ImpressionsDisabled(),
	}
}

// EvaluateFeature returns a struct with the resulting treatment and extra information for the impression
func (e *Evaluator) EvaluateFeature(key string, bucketingKey *string, featureFlag string, attributes map[string]interface{}) *Result {
	before := time.Now()
	splitDto := e.splitStorage.Split(featureFlag)

	if bucketingKey == nil {
		bucketingKey = &key
	}
	result := e.evaluateTreatment(key, *bucketingKey, featureFlag, splitDto, attributes)
	after := time.Now()

	result.EvaluationTime = after.Sub(before)
	return result
}

// EvaluateFeatures returns a struct with the resulting treatment and extra information for the impression
func (e *Evaluator) EvaluateFeatures(key string, bucketingKey *string, featureFlags []string, attributes map[string]interface{}) Results {
	var results = Results{
		Evaluations:    make(map[string]Result),
		EvaluationTime: 0,
	}
	before := time.Now()
	splits := e.splitStorage.FetchMany(featureFlags)

	if bucketingKey == nil {
		bucketingKey = &key
	}
	for _, featureFlag := range featureFlags {
		results.Evaluations[featureFlag] = *e.evaluateTreatment(key, *bucketingKey, featureFlag, splits[featureFlag], attributes)
	}

	after := time.Now()
	results.EvaluationTime = after.Sub(before)
	return results
}

// EvaluateFeatureByFlagSets returns a struct with the resulting treatment and extra information for the impression
func (e *Evaluator) EvaluateFeatureByFlagSets(key string, bucketingKey *string, flagSets []string, attributes map[string]interface{}) Results {
	featureFlagNamesBySets := e.getFeatureFlagNamesByFlagSets(flagSets)
	if len(featureFlagNamesBySets) == 0 {
		return Results{}
	}
	return e.EvaluateFeatures(key, bucketingKey, featureFlagNamesBySets, attributes)
}

// GetFeatureFlagNamesByFlagSets return flags that belong to some flag set
func (e *Evaluator) getFeatureFlagNamesByFlagSets(flagSets []string) []string {
	uniqueFlags := make(map[string]struct{})
	flagsBySets := e.splitStorage.GetNamesByFlagSets(flagSets)
	for set, flags := range flagsBySets {
		if len(flags) == 0 {
			e.logger.Warning(fmt.Sprintf("you passed %s Flag Set that does not contain cached feature flag names, please double check what Flag Sets are in use in the Split user interface.", set))
			continue
		}
		for _, featureFlag := range flags {
			uniqueFlags[featureFlag] = struct{}{}
		}
	}
	flags := make([]string, 0, len(uniqueFlags))
	for flag := range uniqueFlags {
		flags = append(flags, flag)
	}
	return flags
}

// EvaluateDependency SHOULD ONLY BE USED by DependencyMatcher.
// It's used to break the dependency cycle between matchers and evaluators.
func (e *Evaluator) EvaluateDependency(key string, bucketingKey *string, featureFlag string, attributes map[string]interface{}) string {
	res := e.EvaluateFeature(key, bucketingKey, featureFlag, attributes)
	return res.Treatment
}
