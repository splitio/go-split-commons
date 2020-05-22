package redis

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-toolkit/datastructures/set"
	"github.com/splitio/go-toolkit/logging"
	"github.com/splitio/go-toolkit/redis"
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

// incr stores/increments trafficType in Redis
func (r *SplitStorage) incr(trafficType string) error {
	key := strings.Replace(redisTrafficType, "{trafficType}", trafficType, 1)

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
	key := strings.Replace(redisTrafficType, "{trafficType}", trafficType, 1)

	val, _ := r.client.Decr(key)
	if val <= 0 {
		_, err := r.client.Del(key)
		if err != nil {
			r.logger.Verbose(fmt.Sprintf("Error removing trafficType %s in redis", trafficType))
		}
	}
	return nil
}

func (r *SplitStorage) getValues(split []byte) (string, string, error) {
	var tmpSplit map[string]interface{}
	err := json.Unmarshal(split, &tmpSplit)
	if err != nil {
		r.logger.Error("Split Values couldn't be fetched", err)
		return "", "", err
	}
	key := tmpSplit["name"].(string)
	trafficTypeName := tmpSplit["trafficTypeName"].(string)
	return key, trafficTypeName, nil
}

// Put an split object
func (r *SplitStorage) Put(split []byte) error {
	r.mutext.Lock()
	defer r.mutext.Unlock()

	splitName, trafficType, err := r.getValues(split)
	if err != nil {
		r.logger.Error("Split Name & TrafficType couldn't be fetched", err)
		return err
	}

	existing := r.Split(splitName)
	if existing != nil {
		// If it's an update, we decrement the traffic type count of the existing split,
		// and then add the updated one (as part of the normal flow), in case it's different.
		r.decr(existing.TrafficTypeName)
	}

	r.incr(trafficType)

	key := strings.Replace(redisSplit, "{split}", splitName, 1)
	err = r.client.Set(key, string(split), 0)
	if err != nil {
		r.logger.Error("Error saving item", splitName, "in Redis:", err)
	} else {
		r.logger.Verbose("Item saved at key:", splitName)
	}

	return err
}

// PutMany bulk stores splits in redis
func (r *SplitStorage) PutMany(splits []dtos.SplitDTO, changeNumber int64) {
	for _, split := range splits {
		keyToStore := strings.Replace(redisSplit, "{split}", split.Name, 1)
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
	err := r.client.Set(redisSplitTill, changeNumber, 0)
	if err != nil {
		r.logger.Error("Could not update split changenumber")
	}
}

func (r *SplitStorage) remove(keys ...string) error {
	withNamespace := make([]string, len(keys))
	for index, key := range keys {
		withNamespace[index] = strings.Replace(redisSplit, "{split}", key, 1)
	}
	val, err := r.client.Del(withNamespace...)
	if err != nil {
		r.logger.Error("Error removing splits in redis")
		return err
	}
	r.logger.Verbose(val, " splits removed fromr redis")
	return nil
}

// Remove removes split item from redis
func (r *SplitStorage) Remove(splitName string) {
	keyToDelete := strings.Replace(redisSplit, "{split}", splitName, 1)
	_, err := r.client.Del(keyToDelete)
	if err != nil {
		r.logger.Error(fmt.Sprintf("Error deleting split \"%s\".", splitName))
	}
}

// Split fetches a feature in redis and returns a pointer to a split dto
func (r *SplitStorage) Split(feature string) *dtos.SplitDTO {
	keyToFetch := strings.Replace(redisSplit, "{split}", feature, 1)
	val, err := r.client.Get(keyToFetch)

	if err != nil {
		r.logger.Error(fmt.Sprintf("Could not fetch feature \"%s\" from redis: %s", feature, err.Error()))
		return nil
	}

	var split dtos.SplitDTO
	err = json.Unmarshal([]byte(val), &split)
	if err != nil {
		r.logger.Error(fmt.Sprintf("Could not parse feature \"%s\" fetched from redis", feature))
		return nil
	}

	return &split
}

// FetchMany retrieves features from redis storage
func (r *SplitStorage) FetchMany(features []string) map[string]*dtos.SplitDTO {
	keysToFetch := make([]string, 0)
	for _, feature := range features {
		keysToFetch = append(keysToFetch, strings.Replace(redisSplit, "{split}", feature, 1))
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

// All returns a slice of splits dtos.
func (r *SplitStorage) All() []dtos.SplitDTO {
	splits := make([]dtos.SplitDTO, 0)
	keyPattern := strings.Replace(redisSplit, "{split}", "*", 1)
	keys, err := r.client.Keys(keyPattern)
	if err != nil {
		r.logger.Error("Error fetching split keys. Returning empty split list")
		return splits
	}

	// @TODO Change to MGET
	for _, key := range keys {
		raw, err := r.client.Get(key)
		if err != nil {
			r.logger.Error(fmt.Sprintf("Fetching key \"%s\", skipping.", key))
			continue
		}

		var split dtos.SplitDTO
		err = json.Unmarshal([]byte(raw), &split)
		if err != nil {
			r.logger.Error(fmt.Sprintf("Error parsing json for split %s", key))
			continue
		}
		splits = append(splits, split)
	}
	return splits
}

// ChangeNumber returns the latest split changeNumber
func (r *SplitStorage) ChangeNumber() (int64, error) {
	val, err := r.client.Get(redisSplitTill)
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

// SetChangeNumber sets the till value belong to segmentName
func (r *SplitStorage) SetChangeNumber(changeNumber int64) error {
	return r.client.Set(redisSplitTill, changeNumber, 0)
}

// SplitNames returns a slice of strings with all the split names
func (r *SplitStorage) SplitNames() []string {
	splitNames := make([]string, 0)
	keyPattern := strings.Replace(redisSplit, "{split}", "*", 1)
	keys, err := r.client.Keys(keyPattern)
	if err == nil {
		toRemove := strings.Replace(redisSplit, "{split}", "", 1) // Create a string with all the prefix to remove
		for _, key := range keys {
			splitNames = append(splitNames, strings.Replace(key, toRemove, "", 1)) // Extract split name from key
		}
	}
	return splitNames
}

// TrafficTypeExists returns true or false depending on existence and counter
// of trafficType
func (r *SplitStorage) TrafficTypeExists(trafficType string) bool {
	keyToFetch := strings.Replace(redisTrafficType, "{trafficType}", trafficType, 1)
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

// Clear removes all splits from storage
func (r *SplitStorage) Clear() {
}

// SegmentNames returns a slice of strings with all the segment names
func (r *SplitStorage) SegmentNames() *set.ThreadUnsafeSet {
	// NOT USED BY REDIS
	segmentNames := set.NewSet()
	keyPattern := strings.Replace(redisSplit, "{split}", "*", 1)
	keys, err := r.client.Keys(keyPattern)
	if err != nil {
		r.logger.Error("Error fetching split keys. Returning empty segment list")
		return segmentNames
	}

	// @TODO replace to MGET
	for _, key := range keys {
		raw, err := r.client.Get(key)
		if err != nil {
			r.logger.Error(fmt.Sprintf("Fetching key \"%s\", skipping.", key))
			continue
		}

		var split dtos.SplitDTO
		err = json.Unmarshal([]byte(raw), &split)
		if err != nil {
			r.logger.Error(fmt.Sprintf("Error parsing json for split %s", key))
			continue
		}
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

// RegisterSegment add the segment name into redis set
func (r *SplitStorage) RegisterSegment(name string) error {
	// @TODO DEPRECATE
	_, err := r.client.SAdd("SPLITIO.segments.registered", name)
	if err != nil {
		r.logger.Debug("Error saving segment", name, err)
	}
	return err
}

// RawSplits return an slice with Split json representation
func (r *SplitStorage) RawSplits() ([]string, error) {
	// @TODO DEPRECATE
	splitsNames := r.SplitNames()

	toReturn := make([]string, 0)
	for _, splitName := range splitsNames {
		splitJSON, err := r.client.Get(strings.Replace(redisSplit, "{split}", splitName, 1))
		if err != nil {
			r.logger.Error(fmt.Printf("Error fetching split from redis: %s\n", splitName))
			continue
		}
		toReturn = append(toReturn, splitJSON)
	}

	return toReturn, nil
}
