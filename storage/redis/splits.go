package redis

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"

	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-split-commons/v5/storage"
	"github.com/splitio/go-toolkit/v5/datastructures/set"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/splitio/go-toolkit/v5/redis"
)

// SplitStorage is a redis-based implementation of split storage
type SplitStorage struct {
	client *redis.PrefixedRedisClient
	logger logging.LoggerInterface
	mutext *sync.RWMutex
}

// NewSplitStorage creates a new RedisSplitStorage and returns a reference to it
func NewSplitStorage(redisClient *redis.PrefixedRedisClient, logger logging.LoggerInterface) *SplitStorage {
	return &SplitStorage{
		client: redisClient,
		logger: logger,
		mutext: &sync.RWMutex{},
	}
}

// All returns a slice of splits dtos.
func (r *SplitStorage) All() []dtos.SplitDTO {
	keys, err := r.getAllSplitKeys()
	if err != nil {
		r.logger.Error("Error fetching split keys. Returning empty split list: ", err)
		return nil
	}

	if len(keys) == 0 {
		return nil // no splits in cache, nothing to do here
	}

	rawSplits, err := r.client.MGet(keys)
	if err != nil {
		r.logger.Error("Could not get splits")
		return nil
	}

	splits := make([]dtos.SplitDTO, 0, len(rawSplits))
	for idx, raw := range rawSplits {
		var split dtos.SplitDTO
		rawSplit, ok := rawSplits[idx].(string)
		if ok {
			err = json.Unmarshal([]byte(rawSplit), &split)
			if err != nil {
				r.logger.Error(fmt.Sprintf("Error parsing json for split %s", raw))
				continue
			}
		}
		splits = append(splits, split)
	}

	return splits
}

// ChangeNumber returns the latest split changeNumber
func (r *SplitStorage) ChangeNumber() (int64, error) {
	val, err := r.client.Get(KeySplitTill)
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

// FetchMany retrieves features from redis storage
func (r *SplitStorage) FetchMany(features []string) map[string]*dtos.SplitDTO {
	if len(features) == 0 {
		return nil
	}

	keysToFetch := make([]string, 0, len(features))
	for _, feature := range features {
		keysToFetch = append(keysToFetch, strings.Replace(KeySplit, "{split}", feature, 1))
	}
	rawSplits, err := r.client.MGet(keysToFetch)
	if err != nil {
		r.logger.Error(fmt.Sprintf("Could not fetch features from redis: %s", err.Error()))
		return nil
	}

	splits := make(map[string]*dtos.SplitDTO)
	for idx, feature := range features {
		var split *dtos.SplitDTO
		rawSplit, ok := rawSplits[idx].(string)
		if ok {
			err = json.Unmarshal([]byte(rawSplit), &split)
			if err != nil {
				r.logger.Error("Could not parse feature \"%s\" fetched from redis", feature)
				return nil
			}
		}
		splits[feature] = split
	}

	return splits
}

// KillLocally mock
func (r *SplitStorage) KillLocally(splitName string, defaultTreatment string, changeNumber int64) {
	// @TODO Implement for Sync
}

func (r *SplitStorage) fetchCurrentFeatureFlags(toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO) ([]dtos.SplitDTO, error) {
	toAddKeys := make([]string, 0, len(toAdd))
	for idx := range toAdd {
		toAddKeys = append(toAddKeys, strings.Replace(KeySplit, "{split}", toAdd[idx].Name, 1))
	}

	toRemoveKeys := make([]string, 0, len(toRemove))
	for idx := range toRemove {
		toRemoveKeys = append(toRemoveKeys, strings.Replace(KeySplit, "{split}", toRemove[idx].Name, 1))
	}

	allKeys := append(make([]string, 0, len(toAdd)+len(toRemove)), toAddKeys...)
	allKeys = append(allKeys, toRemoveKeys...)
	currentFeatureFlags := make([]dtos.SplitDTO, 0, len(allKeys))

	if len(allKeys) == 0 {
		return currentFeatureFlags, nil
	}

	// Get all the featureFlags involved in the update in order to have the previous version
	toUpdateRaw, err := r.client.MGet(allKeys)
	if err != nil {
		return currentFeatureFlags, fmt.Errorf("error fetching keys to be updated: %w", err)
	}

	for _, raw := range toUpdateRaw {
		asStr, ok := raw.(string)
		if !ok {
			if raw != nil { // object missing in redis
				r.logger.Debug(fmt.Sprintf(
					"Update: ignoring split stored in redis that cannot be parsed for traffic-type updating purposes: [%T] %+v",
					raw, raw,
				))
			}
			continue
		}

		var s dtos.SplitDTO
		err = json.Unmarshal([]byte(asStr), &s)
		if err != nil {
			r.logger.Debug("Update: ignoring split stored in redis that cannot be deserialized for traffic-type updating purposes: ", asStr)
			continue
		}
		currentFeatureFlags = append(currentFeatureFlags, s)
	}

	return currentFeatureFlags, nil
}

// getCurrentSets constructs the current (previous version) before updating of
// the flagsets for all the featureFlags involved in the update to be performed
func getCurrentSets(featureFlags []dtos.SplitDTO) map[string]map[string]struct{} {
	// map[set]map[split] for tracking previous set version before updates
	currentSets := make(map[string]map[string]struct{})

	// the idea is to build the previous version of all the linked feature flag involved to set
	// this is required since we need to track cases where a set created on previous change
	// is no longer linked to the feature flag to be updated
	// map[set1][split1]{}
	// map[set2][split1]{}
	// map[set2][split2]{}
	for _, featureFlag := range featureFlags {
		for _, set := range featureFlag.Sets {
			_, exists := currentSets[set]
			if !exists {
				currentSets[set] = make(map[string]struct{})
			}
			currentSets[set][featureFlag.Name] = struct{}{}
		}
	}

	return currentSets
}

// calculateSets calculates the featureFlags that needs to be removed from sets and the featureFlags that needs to be
// added into the sets
func calculateSets(currentSets map[string]map[string]struct{}, toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO) map[string]map[string]struct{} {
	// map[set]map[ff] for tracking all the sets that feature flags are going to be added
	setsToAdd := make(map[string]map[string]struct{})

	for _, featureFlag := range toAdd {
		// for each feature flag, get all the sets that need to be added
		// map[set1][split1]{}
		// map[set1][split2]{}
		// map[set2][split1]{}
		for _, set := range featureFlag.Sets {
			// at this point if the previous of set is compared against the added one
			// the sets which the feature flag needs to be added and removed can be calculated
			// previous version stored in redis of ff1 has sets: [setA, setB]
			// incomming sets of ff1 are: [setA, setC]
			// e.g:
			// currentSet map[ff1][setA], map[ff1][setB]
			// incommingSets map[setA][ff1], map[setC][ff1]
			// if setA is in previous version and in the incomming one the add is discarded
			// and setA is also removed from currentSet, which means that
			// the remaining items are going to be the ones to be removed
			// if setC is not in previous version, then is added into setsToAdd
			// the new version of ff1 is not linked to setB which is the only item
			// in currentSet
			_, isAlreadyInSet := currentSets[set][featureFlag.Name]
			// if is present, don't perform operation of adding since is already there
			if isAlreadyInSet {
				// clean the featureFlag to avoid unnecesary operate over it
				delete(currentSets[set], featureFlag.Name)
				// if the set is empty, remove it from the dictionary
				if len(currentSets[set]) == 0 {
					delete(currentSets, set)
				}
				continue
			}
			_, exists := setsToAdd[set]
			if !exists {
				setsToAdd[set] = map[string]struct{}{}
			}
			setsToAdd[set][featureFlag.Name] = struct{}{}
		}
	}

	return setsToAdd
}

func updateFeatureFlags(pipeline redis.Pipeline, toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO) (map[string]error, []string) {
	failedToAdd := make(map[string]error)
	addedInPipe := make([]string, 0, len(toAdd))

	for _, split := range toAdd {
		keyToStore := strings.Replace(KeySplit, "{split}", split.Name, 1)
		raw, err := json.Marshal(split)
		if err != nil {
			failedToAdd[split.Name] = fmt.Errorf("failed to serialize split: %w", err)
			continue
		}

		// Attach each Feature Flag update into Redis Pipeline
		pipeline.Set(keyToStore, raw, 0)
		addedInPipe = append(addedInPipe, split.Name)
	}

	if len(toRemove) > 0 {
		toRemoveKeys := make([]string, 0, len(toRemove))
		for idx := range toRemove {
			toRemoveKeys = append(toRemoveKeys, strings.Replace(KeySplit, "{split}", toRemove[idx].Name, 1))
		}
		pipeline.Del(toRemoveKeys...)
	}

	return failedToAdd, addedInPipe
}

func updateFlagSets(pipeline redis.Pipeline, toAdd map[string]map[string]struct{}, toRemove map[string]map[string]struct{}) {
	for set, featureFlags := range toAdd {
		featureFlagsNames := make([]interface{}, 0, len(featureFlags))
		for featureFlag := range featureFlags {
			featureFlagsNames = append(featureFlagsNames, featureFlag)
		}
		pipeline.SAdd(strings.Replace(KeyFlagSet, "{set}", set, 1), featureFlagsNames...)
	}
	for set, featureFlags := range toRemove {
		featureFlagsNames := make([]interface{}, 0, len(featureFlags))
		for featureFlag := range featureFlags {
			featureFlagsNames = append(featureFlagsNames, featureFlag)
		}
		pipeline.SRem(strings.Replace(KeyFlagSet, "{set}", set, 1), featureFlagsNames...)
	}
}

func updateTrafficTypes(pipeline redis.Pipeline, currentFeatureFlags []dtos.SplitDTO, toAdd []dtos.SplitDTO) {
	for _, featureFlag := range currentFeatureFlags {
		pipeline.Decr(strings.Replace(KeyTrafficType, "{trafficType}", featureFlag.TrafficTypeName, 1))
	}

	for idx := range toAdd {
		pipeline.Incr(strings.Replace(KeyTrafficType, "{trafficType}", toAdd[idx].TrafficTypeName, 1))
	}
}

// UpdateWithErrors updates the storage and reports errors on a per-feature basis
// To-be-deprecated: This method should be renamed to `Update` as the current one is removed
func (r *SplitStorage) UpdateWithErrors(toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO, changeNumber int64) error {
	r.mutext.Lock()
	defer r.mutext.Unlock()

	// Gather all the feature flags involved for update operation
	allFeatureFlags, err := r.fetchCurrentFeatureFlags(toAdd, toRemove)
	if err != nil {
		return err
	}

	// Get current sets
	currentSets := getCurrentSets(allFeatureFlags)
	// From featureFlags to be added and removed, calculate feature flags to be added
	// and update currentSets to get only the ones that needs to be removed
	setsToAdd := calculateSets(currentSets, toAdd, toRemove)

	// Instantiating pipeline for adding operations to pipe
	pipeline := r.client.Pipeline()

	// Attach to the pipe all the operations related to featureFlags (Set, Del)
	// addedInPipe are all the featureFlags without Marshal error
	failedToAdd, addedInPipe := updateFeatureFlags(pipeline, toAdd, toRemove)
	// Attach to the pipe all the operations related to flagSets (SAdd, SRem)
	updateFlagSets(pipeline, setsToAdd, currentSets)
	// Attach to the pipe all the operations related to trafficTypes (Incr, Decr)
	updateTrafficTypes(pipeline, allFeatureFlags, toAdd)

	// Execute all the operation attached into the pipe
	result, err := pipeline.Exec()
	// Check general error and logging it
	if err != nil {
		r.logger.Error("Error performing pipeline operation for updating feature flags", err.Error())
	}
	// If the result is at least equals to all the add operations
	// iterate over them and wrap the corresponding error to the
	// linked to the proper feature flag
	if len(result) >= len(addedInPipe) {
		for idx, result := range result[:len(addedInPipe)] {
			err := result.Err()
			if err != nil {
				failedToAdd[addedInPipe[idx]] = fmt.Errorf("failed to store feature flag in redis: %w", err)
			}
		}
	}

	// If the result is at least equals to all the add operations
	// plus the Del, wrap failed removal
	failedToRemove := make(map[string]error)
	if len(toRemove) > 0 && len(result) > len(addedInPipe)+1 {
		count, err := result[len(addedInPipe)].Result()
		if err != nil {
			for idx := range toRemove {
				failedToRemove[toRemove[idx].Name] = fmt.Errorf("failed to remove feature flag from redis: %w", err)
			}
		}
		if count != int64(len(toRemove)) {
			r.logger.Warning(fmt.Sprintf("intended to archive %d splits, but only %d succeeded.", len(toRemove), count))
		}
	}

	// Check if no errors occurs adding and removing featureFlags
	// Set the ChangenUmber
	if len(failedToAdd) == 0 && len(failedToRemove) == 0 {
		err := r.client.Set(KeySplitTill, changeNumber, 0)
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

// Update bulk stores splits in redis
func (r *SplitStorage) Update(toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO, changeNumber int64) {
	if err := r.UpdateWithErrors(toAdd, toRemove, changeNumber); err != nil {
		r.logger.Error("error updating splits: %s", err.Error())
	}
}

// SegmentNames returns a slice of strings with all the segment names
func (r *SplitStorage) SegmentNames() *set.ThreadUnsafeSet {
	segmentNames := set.NewSet()
	splits := r.All()

	for _, split := range splits {
		for _, condition := range split.Conditions {
			for _, matcher := range condition.MatcherGroup.Matchers {
				if matcher.UserDefinedSegment != nil {
					segmentNames.Add(matcher.UserDefinedSegment.SegmentName)
				}
			}
		}
	}
	return segmentNames
}

// SetChangeNumber sets the till value belong to segmentName
func (r *SplitStorage) SetChangeNumber(changeNumber int64) error {
	return r.client.Set(KeySplitTill, changeNumber, 0)
}

func (r *SplitStorage) split(feature string) (*dtos.SplitDTO, error) {
	keyToFetch := strings.Replace(KeySplit, "{split}", feature, 1)
	val, err := r.client.Get(keyToFetch)

	if err != nil {
		return nil, fmt.Errorf("error reading split %s from redis: %w", feature, err)
	}

	var split dtos.SplitDTO
	err = json.Unmarshal([]byte(val), &split)
	if err != nil {
		return nil, fmt.Errorf("Could not parse feature %s fetched from redis: %w", feature, err)
	}

	return &split, nil
}

// Split fetches a feature in redis and returns a pointer to a split dto
func (r *SplitStorage) Split(feature string) *dtos.SplitDTO {
	res, err := r.split(feature)
	if err != nil {
		r.logger.Error(err.Error())
		return nil
	}

	return res
}

// SplitNames returns a slice of strings with all the split names
func (r *SplitStorage) SplitNames() []string {
	//keys, err := r.client.Keys(strings.Replace(KeySplit, "{split}", "*", 1))
	keys, err := r.getAllSplitKeys()
	if err != nil {
		r.logger.Error("error fetching split names form redis: ", err)
		return nil
	}

	splitNames := make([]string, 0, len(keys))
	toRemove := strings.Replace(KeySplit, "{split}", "", 1) // Create a string with all the prefix to remove
	for _, key := range keys {
		splitNames = append(splitNames, strings.Replace(key, toRemove, "", 1)) // Extract split name from key
	}
	return splitNames
}

// TrafficTypeExists returns true or false depending on existence and counter
// of trafficType
func (r *SplitStorage) TrafficTypeExists(trafficType string) bool {
	keyToFetch := strings.Replace(KeyTrafficType, "{trafficType}", trafficType, 1)
	res, err := r.client.Get(keyToFetch)

	if err != nil {
		r.logger.Error(fmt.Sprintf("Could not fetch trafficType \"%s\" from redis: %s", trafficType, err.Error()))
		return false
	}

	val, err := strconv.ParseInt(res, 10, 64)
	if err != nil {
		r.logger.Error("TrafficType could not be converted")
		return false
	}
	return val > 0
}

func (r *SplitStorage) getAllSplitKeys() ([]string, error) {
	if !r.client.ClusterMode() {
		return r.client.Keys(strings.Replace(KeySplit, "{split}", "*", 1))
	}

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
		if strings.HasPrefix(key, "SPLITIO.split.") {
			result = append(result, key)
		}
	}

	return result, nil
}

var _ storage.SplitStorage = (*SplitStorage)(nil)
