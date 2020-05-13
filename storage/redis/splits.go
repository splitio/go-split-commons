package redis

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-toolkit/datastructures/set"
	"github.com/splitio/go-toolkit/logging"
	"github.com/splitio/go-toolkit/redis"
)

// SplitStorage is a redis-based implementation of split storage
type SplitStorage struct {
	client *redis.PrefixedRedisClient
	logger logging.LoggerInterface
}

// NewSplitStorage creates a new RedisSplitStorage and returns a reference to it
func NewSplitStorage(redisClient *redis.PrefixedRedisClient, logger logging.LoggerInterface) *SplitStorage {
	return &SplitStorage{
		client: redisClient,
		logger: logger,
	}
}

// Get fetches a feature in redis and returns a pointer to a split dto
func (r *SplitStorage) Get(feature string) *dtos.SplitDTO {
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

// Till returns the latest split changeNumber
func (r *SplitStorage) Till() int64 {
	val, err := r.client.Get(redisSplitTill)
	if err != nil {
		return -1
	}
	asInt, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		r.logger.Error("Could not parse Till value from redis")
		return -1
	}
	return asInt
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

// SegmentNames returns a slice of strings with all the segment names
func (r *SplitStorage) SegmentNames() *set.ThreadUnsafeSet {
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

// GetAll returns a slice of splits dtos.
func (r *SplitStorage) GetAll() []dtos.SplitDTO {
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

// Clear removes all splits from storage
func (r *SplitStorage) Clear() {
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
