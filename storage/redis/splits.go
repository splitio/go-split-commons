package redis

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/splitio/go-split-commons/v4/dtos"
	"github.com/splitio/go-split-commons/v4/storage"
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
	splits := make([]dtos.SplitDTO, 0)
	keyPattern := strings.Replace(KeySplit, "{split}", "*", 1)
	keys, err := r.client.Keys(keyPattern)
	if err != nil {
		r.logger.Error("Error fetching split keys. Returning empty split list")
		return splits
	}

	rawSplits, err := r.client.MGet(keys)
	if err != nil {
		r.logger.Error("Could not get splits")
		return splits
	}
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
	keysToFetch := make([]string, 0)
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

// incr stores/increments trafficType in Redis
func (r *SplitStorage) incr(trafficType string) error {
	key := strings.Replace(KeyTrafficType, "{trafficType}", trafficType, 1)

	_, err := r.client.Incr(key)
	if err != nil {
		r.logger.Error(fmt.Sprintf("Error storing trafficType %s in redis", trafficType))
		r.logger.Error(err)
		return errors.New("Error incrementing trafficType")
	}
	return nil
}

// decr decrements trafficType count in Redis
func (r *SplitStorage) decr(trafficType string) error {
	key := strings.Replace(KeyTrafficType, "{trafficType}", trafficType, 1)

	val, _ := r.client.Decr(key)
	if val <= 0 {
		_, err := r.client.Del(key)
		if err != nil {
			r.logger.Verbose(fmt.Sprintf("Error removing trafficType %s in redis", trafficType))
		}
	}
	return nil
}

// Update bulk stores splits in redis
func (r *SplitStorage) Update(toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO, changeNumber int64) {
	// TODO(mredolatti): This should be implemented with a pipeline
	r.mutext.Lock()
	defer r.mutext.Unlock()

	toAddKeys := make([]string, 0, len(toAdd))
	toIncrKeys := make([]string, 0, len(toAdd))
	for _, split := range toAdd {
		toAddKeys = append(toAddKeys, strings.Replace(KeySplit, "{split}", split.Name, 1))
		toIncrKeys = append(toIncrKeys, strings.Replace(KeyTrafficType, "{trafficType}", split.TrafficTypeName, 1))
	}

	toRemoveKeys := make([]string, 0, len(toRemove))
	for _, split := range toRemove {
		toRemoveKeys = append(toRemoveKeys, strings.Replace(KeySplit, "{split}", split.Name, 1))
	}

	// Gather all the EXISTING traffic types (if any) of all the added and removed splits
	// we then decrement them and, increment the new ones
	// \{
	allKeys := append(make([]string, 0, len(toAdd)+len(toRemove)), toAddKeys...)
	allKeys = append(allKeys, toRemoveKeys...)

	if len(allKeys) > 0 {
		toUpdateRaw, err := r.client.MGet(allKeys)
		if err != nil {
			r.logger.Error("error fetching keys to be updated:", err)
			return
		}

		ttsToDecr := make([]string, 0, len(allKeys))
		for _, raw := range toUpdateRaw {
			asStr, ok := raw.(string)
			if !ok {
				continue
			}

			var s dtos.SplitDTO
			err = json.Unmarshal([]byte(asStr), &s)
			if err != nil {
				r.logger.Error("Update: ignoring split stored in redis cannot be de-serialized: ", asStr)
				continue
			}

			ttsToDecr = append(ttsToDecr, s.TrafficTypeName)
		}

		for _, tt := range ttsToDecr {
			r.client.Decr(strings.Replace(KeyTrafficType, "{trafficType}", tt, 1))
		}
	}
	// \}

	// The next operations could be implemented in a pipeline, improving the performance
	// of this operation (or even a Tx for even better consistency on splits vs CN).
	// \{
	for _, ttKey := range toIncrKeys {
		r.client.Incr(ttKey)
	}

	for _, split := range toAdd {
		keyToStore := strings.Replace(KeySplit, "{split}", split.Name, 1)
		raw, err := json.Marshal(split)
		if err != nil {
			r.logger.Error(fmt.Sprintf("Could not dump feature \"%s\" to json", split.Name))
			continue
		}

		err = r.client.Set(keyToStore, raw, 0)
		if err != nil {
			r.logger.Error(fmt.Sprintf("Could not store split \"%s\" in redis: %s", split.Name, err.Error()))
		}
	}

	if len(toRemoveKeys) > 0 {
		res := r.client.Client.Del(toRemoveKeys...)
		if res.Err() != nil {
			r.logger.Error("error removing some keys")
		}
	}

	err := r.client.Set(KeySplitTill, changeNumber, 0)
	if err != nil {
		r.logger.Error("Could not update split changenumber")
	}
}

// Remove removes split item from redis
// Deprecated: Though public, this method does not follow a transactional nature and should be avoided whenever possible.
func (r *SplitStorage) Remove(splitName string) {
	r.mutext.Lock()
	defer r.mutext.Unlock()
	keyToDelete := strings.Replace(KeySplit, "{split}", splitName, 1)
	existing, _ := r.split(splitName)
	if existing == nil {
		r.logger.Warning("Tried to delete split " + splitName + " which doesn't exist. ignoring")
		return
	}
	r.decr(existing.TrafficTypeName)
	_, err := r.client.Del(keyToDelete)
	if err != nil {
		r.logger.Error(fmt.Sprintf("Error deleting split \"%s\".", splitName))
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
	splitNames := make([]string, 0)
	keyPattern := strings.Replace(KeySplit, "{split}", "*", 1)
	keys, err := r.client.Keys(keyPattern)
	if err == nil {
		toRemove := strings.Replace(KeySplit, "{split}", "", 1) // Create a string with all the prefix to remove
		for _, key := range keys {
			splitNames = append(splitNames, strings.Replace(key, toRemove, "", 1)) // Extract split name from key
		}
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

var _ storage.SplitStorage = (*SplitStorage)(nil)
