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

type keys struct {
	toAddKeys    []string
	toRemoveKeys []string
	toIncrKeys   []string
	allKeys      []string
}

func gatherAllKeys(toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO) keys {
	toAddKeys := make([]string, 0, len(toAdd))
	toIncrKeys := make([]string, 0, len(toAdd))
	for idx := range toAdd {
		toAddKeys = append(toAddKeys, strings.Replace(KeySplit, "{split}", toAdd[idx].Name, 1))
		toIncrKeys = append(toIncrKeys, strings.Replace(KeyTrafficType, "{trafficType}", toAdd[idx].TrafficTypeName, 1))
	}

	toRemoveKeys := make([]string, 0, len(toRemove))
	for idx := range toRemove {
		toRemoveKeys = append(toRemoveKeys, strings.Replace(KeySplit, "{split}", toRemove[idx].Name, 1))
	}

	// Gather all the EXISTING traffic types (if any) of all the added and removed splits
	// we then decrement them and, increment the new ones
	// \{
	allKeys := append(make([]string, 0, len(toAdd)+len(toRemove)), toAddKeys...)
	allKeys = append(allKeys, toRemoveKeys...)

	return keys{
		toAddKeys:    toAddKeys,
		toRemoveKeys: toRemoveKeys,
		toIncrKeys:   toIncrKeys,
		allKeys:      allKeys,
	}
}

func (r *SplitStorage) updateTrafficTypes(keys keys) (map[string]map[string]interface{}, error) {
	// map[split]map[set] for tracking previous set version before updates
	currentSets := make(map[string]map[string]interface{})

	// Instantiating pipeline for adding operations to pipe
	pipeline := r.client.Pipeline()

	if len(keys.allKeys) > 0 {
		toUpdateRaw, err := r.client.MGet(keys.allKeys)
		if err != nil {
			return currentSets, fmt.Errorf("error fetching keys to be updated: %w", err)
		}

		ttsToDecr := make([]string, 0, len(keys.allKeys))
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

			ttsToDecr = append(ttsToDecr, s.TrafficTypeName)

			// the idea is to build the previous version of all the linked feature flag involved to set
			// this is required since we need to track cases where a set created on previous change
			// is no longer linked to the feature flag to be updated
			// map[split1][set1]{}
			// map[split1][set2]{}
			// map[split2][set2]{}
			for _, set := range s.Sets {
				_, exists := currentSets[s.Name]
				if !exists {
					currentSets[s.Name] = make(map[string]interface{})
				}
				currentSets[s.Name][set] = struct{}{}
			}
		}

		for _, tt := range ttsToDecr {
			pipeline.Decr(strings.Replace(KeyTrafficType, "{trafficType}", tt, 1))
		}
	}
	// \}

	// The next operations could be implemented in a pipeline, improving the performance
	// of this operation (or even a Tx for even better consistency on splits vs CN).
	// \{
	for _, ttKey := range keys.toIncrKeys {
		pipeline.Incr(ttKey)
	}

	// Execution of all Decr and Incr
	pipeline.Exec()

	return currentSets, nil
}

func (r *SplitStorage) updateSets(toAdd map[string]map[string]interface{}, toRemove map[string][]interface{}) {
	// Instantiating pipeline for adding operations to pipe
	pipeline := r.client.Pipeline()

	for set := range toAdd {
		splitsToAdd := make([]interface{}, 0, len(toAdd[set]))
		for split := range toAdd[set] {
			splitsToAdd = append(splitsToAdd, split)
		}
		pipeline.SAdd(strings.Replace(KeyFlagSet, "{set}", set, 1), splitsToAdd...)
	}
	for set, splits := range toRemove {
		pipeline.SRem(strings.Replace(KeyFlagSet, "{set}", set, 1), splits...)
	}
	// Performing all the Flag Sets operations attached into pipeline, that means
	// SAdd and SRem
	_, err := pipeline.Exec()
	if err != nil {
		r.logger.Error("Error updating flag sets", err.Error())
	}
}

// UpdateWithErrors updates the storage and reports errors on a per-feature basis
// To-be-deprecated: This method should be renamed to `Update` as the current one is removed
func (r *SplitStorage) UpdateWithErrors(toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO, changeNumber int64) error {
	r.mutext.Lock()
	defer r.mutext.Unlock()

	// Gather All Keys for perform Redis operations following Redis template
	keys := gatherAllKeys(toAdd, toRemove)
	currentSets, err := r.updateTrafficTypes(keys)
	if err != nil {
		return err
	}

	// map[set]map[ff] for tracking all the sets that feature flags are going to be added
	setsToAdd := make(map[string]map[string]interface{})
	// map[set][]ff for tracking all the feature flags that are going to be removed from a set
	ffToRemoveFromSet := make(map[string][]interface{})

	failedToAdd := make(map[string]error)

	// Instantiating pipeline for adding operations to pipe
	pipeline := r.client.Pipeline()

	for _, split := range toAdd {
		keyToStore := strings.Replace(KeySplit, "{split}", split.Name, 1)
		raw, err := json.Marshal(split)
		if err != nil {
			failedToAdd[split.Name] = fmt.Errorf("failed to serialize split: %w", err)
			continue
		}

		// whats happen if err?
		// for each feature flag we want to know all the sets that needs to be added
		// map[set1][split1]{}
		// map[set1][split2]{}
		// map[set2][split1]{}
		for _, set := range split.Sets {
			_, exists := setsToAdd[set]
			if !exists {
				setsToAdd[set] = map[string]interface{}{}
			}
			setsToAdd[set][split.Name] = struct{}{}
		}
		// at this point we need to compare the previous sets with the ones that are going to be added
		// e.g:
		// current version stored in redis of ff1 has sets: [setA, setB]
		// updated version of ff1 has sets: [setA, setC]
		// currentSet map[ff1][setA], map[ff1][setB]
		// setsToAdd map[setA][ff1], map[setC][ff1]
		// the new version of ff1 is not linked to setB
		// for each ff, iterate over each set, check if is present on new update
		for set := range currentSets[split.Name] {
			_, present := setsToAdd[set][split.Name]
			if present {
				// if is present, don't perform operation of adding since is already there
				delete(setsToAdd[set], split.Name)
			} else {
				// if not, we should track a removal since is not longer linked to the split
				ffToRemoveFromSet[set] = append(ffToRemoveFromSet[set], split.Name)
			}
		}

		// Attach each Feature Flag update into Redis Pipeline
		pipeline.Set(keyToStore, raw, 0)
	}

	// Execution of all the updates to feature flags
	resAdd, err := pipeline.Exec()
	if len(resAdd) != len(toAdd) || err != nil {
		r.logger.Error("Error updating feature flags")
		if err != nil {
			r.logger.Error("Reason", err.Error())
		}
	}
	for idx, result := range resAdd {
		err := result.Err()
		if err != nil {
			failedToAdd[toAdd[idx].Name] = fmt.Errorf("failed to store split in redis: %w", err)
		}
	}

	// at the end we need to iterate over the feature flags that are going to be removed
	// in order to clean the sets for those
	for _, splitToRemove := range toRemove {
		// for each set that has a feature flag linked
		// add the delete operation for the set
		for _, set := range splitToRemove.Sets {
			_, present := currentSets[set][splitToRemove.Name]
			if present {
				ffToRemoveFromSet[set] = append(ffToRemoveFromSet[set], splitToRemove.Name)
			}
		}
	}

	failedToRemove := make(map[string]error)
	if len(keys.toRemoveKeys) > 0 {
		count, err := r.client.Del(keys.toRemoveKeys...)
		if err != nil {
			for idx := range toRemove {
				failedToRemove[toRemove[idx].Name] = fmt.Errorf("failed to remove split from redis: %w", err)
			}
		}

		if count != int64(len(keys.toRemoveKeys)) {
			r.logger.Warning(fmt.Sprintf("intended to archive %d splits, but only %d succeeded.", len(keys.toRemoveKeys), count))
		}
	}

	// At this stage we are ready to update flagSets
	r.updateSets(setsToAdd, ffToRemoveFromSet)

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
