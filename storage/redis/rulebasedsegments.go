package redis

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"

	"github.com/splitio/go-split-commons/v8/dtos"
	"github.com/splitio/go-split-commons/v8/engine/grammar/constants"
	"github.com/splitio/go-split-commons/v8/storage"
	"github.com/splitio/go-toolkit/v5/datastructures/set"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/splitio/go-toolkit/v5/redis"
)

const rbsKey = "{rbsegment}"

// RuleBasedSegmentStorage is a redis-based implementation of rule-based segment storage
type RuleBasedSegmentStorage struct {
	client *redis.PrefixedRedisClient
	logger logging.LoggerInterface
	mutext *sync.RWMutex
}

// NewSplitStorage creates a new RedisSplitStorage and returns a reference to it
func NewRuleBasedStorage(redisClient *redis.PrefixedRedisClient, logger logging.LoggerInterface) *RuleBasedSegmentStorage {
	return &RuleBasedSegmentStorage{
		client: redisClient,
		logger: logger,
		mutext: &sync.RWMutex{},
	}
}

// ChangeNumber returns the latest rule-based changeNumber
func (r *RuleBasedSegmentStorage) ChangeNumber() (int64, error) {
	val, err := r.client.Get(KeyRuleBasedSegmentTill)
	if err != nil {
		return -1, err
	}
	asInt, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		r.logger.Error("Could not parse Till value from redis")
		return -1, err
	}
	return asInt, nil
}

// SetChangeNumber returns the latest rule-based segment changeNumber
func (r *RuleBasedSegmentStorage) SetChangeNumber(till int64) error {
	return r.client.Set(KeyRuleBasedSegmentTill, till, 0)
}

func (r *RuleBasedSegmentStorage) ruleBasedSegment(ruleBased string) (*dtos.RuleBasedSegmentDTO, error) {
	keyToFetch := strings.Replace(KeyRuleBasedSegment, rbsKey, ruleBased, 1)
	val, err := r.client.Get(keyToFetch)

	if err != nil {
		return nil, fmt.Errorf("error reading rule-based segment %s from redis: %w", ruleBased, err)
	}

	var ruleBasedSegment dtos.RuleBasedSegmentDTO
	err = json.Unmarshal([]byte(val), &ruleBasedSegment)
	if err != nil {
		return nil, fmt.Errorf("could not parse rule-based segment %s fetched from redis: %w", ruleBased, err)
	}

	return &ruleBasedSegment, nil
}

// GetRuleBasedSegmentByName fetches a rule-based segment in redis and returns a pointer to a rule-based segment dto
func (r *RuleBasedSegmentStorage) GetRuleBasedSegmentByName(ruleBased string) (*dtos.RuleBasedSegmentDTO, error) {
	return r.ruleBasedSegment(ruleBased)
}

// All fetches all rule-based segments in redis and returns every rule-based segment
func (r *RuleBasedSegmentStorage) All() []dtos.RuleBasedSegmentDTO {
	keys, err := r.ruleBasedSegmentKeys()
	if err != nil {
		r.logger.Error("Error fetching rule-based segment keys. Returning empty rule-based segment list: ", err)
		return nil
	}

	if len(keys) == 0 {
		return nil // no rule-based segment in cache, nothing to do here
	}

	rawRuleBasedSegments, err := r.client.MGet(keys)
	if err != nil {
		r.logger.Error("Could not get rule-based segments")
		return nil
	}

	ruleBasedSegments := make([]dtos.RuleBasedSegmentDTO, 0, len(rawRuleBasedSegments))
	for idx, raw := range rawRuleBasedSegments {
		var ruleBasedSegment dtos.RuleBasedSegmentDTO
		rawSplit, ok := rawRuleBasedSegments[idx].(string)
		if ok {
			err = json.Unmarshal([]byte(rawSplit), &ruleBasedSegment)
			if err != nil {
				r.logger.Error(fmt.Sprintf("Error parsing json for rule-based segment %s", raw))
				continue
			}
		}
		ruleBasedSegments = append(ruleBasedSegments, ruleBasedSegment)
	}

	return ruleBasedSegments
}

func (r *RuleBasedSegmentStorage) RuleBasedSegmentNames() ([]string, error) {
	keys, err := r.ruleBasedSegmentKeys()
	if err != nil {
		return nil, err
	}
	return cleanPrefixedKeys(keys, strings.Replace(KeyRuleBasedSegment, rbsKey, "", 1)), nil
}

func (r *RuleBasedSegmentStorage) ruleBasedSegmentKeys() ([]string, error) {
	if !r.client.ClusterMode() {
		var cursor uint64
		ruleBasedSegmentKeys := make([]string, 0)
		scanKey := strings.Replace(KeyRuleBasedSegment, rbsKey, "*", 1)
		for {
			keys, rCursor, err := r.client.Scan(cursor, scanKey, DefaultScanCount)
			if err != nil {
				return nil, err
			}

			cursor = rCursor
			ruleBasedSegmentKeys = append(ruleBasedSegmentKeys, keys...)

			if cursor == 0 {
				break
			}
		}
		return ruleBasedSegmentKeys, nil
	}
	return r.ruleBasedSegmentsKeysClusterMode()
}

func (r *RuleBasedSegmentStorage) ruleBasedSegmentsKeysClusterMode() ([]string, error) {
	// the hashtag is bundled in the prefix, so it will automatically be added,
	// and we'll get the slot where all the redis keys are bound
	slot, err := r.client.ClusterSlotForKey("__DUMMY__")
	if err != nil {
		return nil, fmt.Errorf("error getting slot (cluster mode): %w", err)
	}

	count, err := r.client.ClusterCountKeysInSlot(int(slot))
	if err != nil {
		return nil, fmt.Errorf("error fetching number of keys in slot (cluster mode): %w", err)
	}

	if count == 0 { // odd but happens :shrug:
		count = math.MaxInt16
	}

	keys, err := r.client.ClusterKeysInSlot(int(slot), int(count))
	if err != nil {
		return nil, fmt.Errorf("error fetching of keys in slot (cluster mode): %w", err)
	}

	result := make([]string, 0, len(keys))
	for _, key := range keys {
		if strings.HasPrefix(key, "SPLITIO.rbsegment.") {
			result = append(result, key)
		}
	}

	return result, nil
}

func (r *RuleBasedSegmentStorage) Contains(ruleBasedSegmentNames []string) bool {

	keys, err := r.RuleBasedSegmentNames()
	if err != nil {
		return false
	}

	keySet := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		keySet[k] = struct{}{}
	}

	for _, name := range ruleBasedSegmentNames {
		if _, exists := keySet[name]; !exists {
			return false
		}
	}
	return true
}

// Segments returns a slice with the names of all segments referenced in rule-based
func (r *RuleBasedSegmentStorage) Segments() *set.ThreadUnsafeSet {
	segments := set.NewSet()

	ruleBasedSegments := r.All()
	for _, ruleBased := range ruleBasedSegments {
		for _, condition := range ruleBased.Conditions {
			for _, matcher := range condition.MatcherGroup.Matchers {
				if matcher.UserDefinedSegment != nil && matcher.MatcherType == constants.MatcherTypeInSegment {
					segments.Add(matcher.UserDefinedSegment.SegmentName)
				}
			}
		}
		for _, excluded := range ruleBased.Excluded.Segments {
			if excluded.Type == dtos.TypeStandard {
				segments.Add(excluded.Name)
			}
		}
	}
	return segments
}

// LargeSegments returns a slice with the names of all large segments referenced in rule-based
func (r *RuleBasedSegmentStorage) LargeSegments() *set.ThreadUnsafeSet {
	largeSegments := set.NewSet()

	ruleBasedSegments := r.All()
	for _, ruleBased := range ruleBasedSegments {
		for _, condition := range ruleBased.Conditions {
			for _, matcher := range condition.MatcherGroup.Matchers {
				if matcher.UserDefinedLargeSegment != nil {
					largeSegments.Add(matcher.UserDefinedLargeSegment.LargeSegmentName)
				}
			}
		}
		for _, excluded := range ruleBased.Excluded.Segments {
			if excluded.Type == dtos.TypeLarge {
				largeSegments.Add(excluded.Name)
			}
		}
	}
	return largeSegments
}

// UpdateWithErrors updates the storage and reports errors on a per-rule-based segment basis
func (r *RuleBasedSegmentStorage) Update(toAdd []dtos.RuleBasedSegmentDTO, toRemove []dtos.RuleBasedSegmentDTO, changeNumber int64) error {
	r.mutext.Lock()
	defer r.mutext.Unlock()

	// Gather all the rule-based segments involved for update operation
	allRuleBasedSegments, err := r.fetchCurrentRuleBasedSegments(toAdd, toRemove)
	if err != nil {
		return err
	}
	if allRuleBasedSegments == nil {
		return nil
	}

	// Instantiating pipeline for adding operations to pipe
	pipeline := r.client.Pipeline()

	// Attach to the pipe all the operations related to rule-based segments (Set, Del)
	// addedInPipe are all the rule-based Segments without Marshal error
	failedToMarshal, addedInPipe := updateRuleBasedSegments(pipeline, toAdd, toRemove)

	// Execute all the operation attached into the pipe
	failedToAdd, failedToRemove := r.executePipeline(pipeline, addedInPipe, toRemove)

	for name, err := range failedToMarshal {
		failedToAdd[name] = err
	}

	// Check if no errors occurs adding and removing featureFlags
	// Set the ChangenUmber
	if len(failedToAdd) == 0 && len(failedToRemove) == 0 {
		err := r.client.Set(KeyRuleBasedSegmentTill, changeNumber, 0)
		if err != nil {
			return ErrChangeNumberUpdateFailed
		}
		return nil
	}

	return &UpdateError{
		FailedToAdd:    failedToAdd,
		FailedToRemove: failedToRemove,
	}
}

func (r *RuleBasedSegmentStorage) fetchCurrentRuleBasedSegments(toAdd []dtos.RuleBasedSegmentDTO, toRemove []dtos.RuleBasedSegmentDTO) ([]dtos.RuleBasedSegmentDTO, error) {
	if len(toAdd)+len(toRemove) == 0 {
		return nil, nil
	}

	keys := make([]string, 0, len(toAdd)+len(toRemove))
	for _, source := range [][]dtos.RuleBasedSegmentDTO{toAdd, toRemove} {
		for idx := range source {
			keys = append(keys, strings.Replace(KeyRuleBasedSegment, rbsKey, source[idx].Name, 1))
		}
	}

	currentRuleBasedSegments := make([]dtos.RuleBasedSegmentDTO, 0, len(keys))
	// Get all the rule-based segments involved in the update in order to have the previous version
	toUpdateRaw, err := r.client.MGet(keys)
	if err != nil {
		return currentRuleBasedSegments, fmt.Errorf("error fetching keys to be updated: %w", err)
	}

	for _, raw := range toUpdateRaw {
		asStr, ok := raw.(string)
		if !ok {
			if raw != nil { // object missing in redis
				r.logger.Debug(fmt.Sprintf(
					"Update: ignoring rule-based stored in redis that cannot be parsed: [%T] %+v",
					raw, raw,
				))
			}
			continue
		}

		var s dtos.RuleBasedSegmentDTO
		err = json.Unmarshal([]byte(asStr), &s)
		if err != nil {
			r.logger.Debug("Update: ignoring rule-based segment stored in redis that cannot be deserialized: ", asStr)
			continue
		}
		currentRuleBasedSegments = append(currentRuleBasedSegments, s)
	}

	return currentRuleBasedSegments, nil
}

func (r *RuleBasedSegmentStorage) ReplaceAll(toAdd []dtos.RuleBasedSegmentDTO, changeNumber int64) error {
	toRemove := make([]dtos.RuleBasedSegmentDTO, 0)
	toRemove = append(toRemove, r.All()...)

	return r.Update(toAdd, toRemove, changeNumber)
}

func (r *RuleBasedSegmentStorage) executePipeline(pipeline redis.Pipeline, toAdd []string, toRemove []dtos.RuleBasedSegmentDTO) (map[string]error, map[string]error) {
	failedToAdd := make(map[string]error)
	failedToRemove := make(map[string]error)

	result, err := pipeline.Exec()
	// Check general error and logging it
	if err != nil {
		r.logger.Error("Error performing pipeline operation for updating rule-based segment", err.Error())
	}
	// If the result is at least equals to all the add operations
	// iterate over them and wrap the corresponding error to the
	// linked to the proper rule-based segment
	if len(result) >= len(toAdd) {
		for idx, result := range result[:len(toAdd)] {
			err := result.Err()
			if err != nil {
				failedToAdd[toAdd[idx]] = fmt.Errorf("failed to store rule-based segment in redis: %w", err)
			}
		}
	}

	// If the result is at least equals to all the add operations
	// plus the Del, wrap failed removal
	if len(toRemove) > 0 && len(result) > len(toAdd)+1 {
		count, err := result[len(toAdd)].Result()
		if err != nil {
			for idx := range toRemove {
				failedToRemove[toRemove[idx].Name] = fmt.Errorf("failed to remove rule-based segment from redis: %w", err)
			}
		}
		if count != int64(len(toRemove)) {
			r.logger.Warning(fmt.Sprintf("intended to archive %d rule-based segments, but only %d succeeded.", len(toRemove), count))
		}
	}
	return failedToAdd, failedToRemove
}

func (r *RuleBasedSegmentStorage) FetchMany(names []string) map[string]*dtos.RuleBasedSegmentDTO {
	if len(names) == 0 {
		return nil
	}

	keysToFetch := make([]string, 0, len(names))
	for _, name := range names {
		keysToFetch = append(keysToFetch, strings.Replace(KeyRuleBasedSegment, "{rbsegment}", name, 1))
	}
	rawRBS, err := r.client.MGet(keysToFetch)
	if err != nil {
		r.logger.Error(fmt.Sprintf("Could not fetch rule-based segments from redis: %s", err.Error()))
		return nil
	}

	rbs := make(map[string]*dtos.RuleBasedSegmentDTO)
	for idx, rb := range names {
		var rbSegment *dtos.RuleBasedSegmentDTO
		rawRBSegment, ok := rawRBS[idx].(string)
		if ok {
			err = json.Unmarshal([]byte(rawRBSegment), &rbSegment)
			if err != nil {
				r.logger.Error("Could not parse rule-based segment \"%s\" fetched from redis", rb)
				return nil
			}
		}
		rbs[rb] = rbSegment
	}

	return rbs
}

var _ storage.RuleBasedSegmentsStorage = (*RuleBasedSegmentStorage)(nil)
