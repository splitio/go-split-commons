package redis

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-toolkit/datastructures/set"
	"github.com/splitio/go-toolkit/logging"
	"github.com/splitio/go-toolkit/redis"
)

// SegmentStorage is a redis implementation of a storage for segments
type SegmentStorage struct {
	client redis.PrefixedRedisClient
	logger logging.LoggerInterface
}

// NewSegmentStorage creates a new RedisSegmentStorage and returns a reference to it
func NewSegmentStorage(redisClient *redis.PrefixedRedisClient, logger logging.LoggerInterface) *SegmentStorage {
	return &SegmentStorage{
		client: *redisClient,
		logger: logger,
	}
}

// MISSING UPDATE

// Put adds a new segment
func (r *SegmentStorage) Put(name string, segment *set.ThreadUnsafeSet, till int64) {
}

// ChangeNumber returns the changeNumber for a particular segment
func (r *SegmentStorage) ChangeNumber(segmentName string) (int64, error) {
	segmentKey := strings.Replace(redisSegmentTill, "{segment}", segmentName, 1)
	tillStr, err := r.client.Get(segmentKey)
	if err != nil {
		return -1, err
	}

	asInt, err := strconv.ParseInt(tillStr, 10, 64)
	if err != nil {
		r.logger.Error("Error retrieving till. Returning -1: ", err.Error())
		return -1, err
	}
	return asInt, nil
}

// SetChangeNumber sets the till value belong to segmentName
func (r *SegmentStorage) SetChangeNumber(segmentName string, changeNumber int64) error {
	segmentKey := strings.Replace(redisSegmentTill, "{segment}", segmentName, 1)
	return r.client.Set(segmentKey, changeNumber, 0)
}

// Remove deletes a segment from the in-memmory storage
func (r *SegmentStorage) Remove(segmentName string) {
}

// Clear removes all splits from storage
func (r *SegmentStorage) Clear() {
}

// Get returns a segment wrapped in a set
func (r *SegmentStorage) Get(segmentName string) *set.ThreadUnsafeSet {
	// @TODO replace to SISMember
	// @TODO replace to IsInSegment
	keyToFetch := strings.Replace(redisSegment, "{segment}", segmentName, 1)
	segmentKeys, err := r.client.SMembers(keyToFetch)
	if len(segmentKeys) <= 0 {
		r.logger.Warning(fmt.Sprintf("Nonexsitant segment requested: \"%s\"", segmentName))
		return nil
	}
	if err != nil {
		r.logger.Error(fmt.Sprintf("Error retrieving members from set %s", segmentName))
		return nil
	}
	segment := set.NewSet()
	for _, member := range segmentKeys {
		segment.Add(member)
	}
	return segment
}

// Update updates segment
func (r *SegmentStorage) Update(segmentName string, toAdd []string, toRemove []string, till int64) error {
	var err error
	segmentKey := strings.Replace(redisSegment, "{segment}", segmentName, 1)

	if len(toAdd) > 0 {
		r.logger.Debug("Adding to segment", segmentName)
		_keys := make([]interface{}, len(toAdd))
		for i, v := range toAdd {
			_keys[i] = v
		}
		r.logger.Verbose(_keys...)
		_, err = r.client.SAdd(segmentKey, _keys...)
		if err != nil {
			return err
		}
	}
	if len(toRemove) > 0 {
		r.logger.Debug("Removing from segment", segmentName)
		_, err := r.client.SRem(segmentKey, toRemove...)
		if err != nil {
			return err
		}
	}
	return nil
}

/*
// RemoveFromSegment removes a list of keys (strings)
func (r *SegmentStorage) RemoveFromSegment(segmentName string, keys []string) error {
	r.logger.Debug("Removing from segment", segmentName)
	if len(keys) == 0 {
		return nil
	}
	segmentKey := strings.Replace(redisSegment, "{segment}", segmentName, 1)
	_, err := r.client.SRem(segmentKey, keys...)
	return err
}

// AddToSegment adds a list of keys (strings)
func (r *SegmentStorage) AddToSegment(segmentName string, keys []string) error {
	r.logger.Debug("Adding to segment", segmentName)
	if len(keys) == 0 {
		return nil
	}
	_keys := make([]interface{}, len(keys))
	for i, v := range keys {
		_keys[i] = v
	}
	r.logger.Verbose(_keys...)
	segmentKey := strings.Replace(redisSegment, "{segment}", segmentName, 1)
	_, err := r.client.SAdd(segmentKey, _keys...)
	return err
}
*/

// CountActiveKeys count the numbers of keys in segmentName
func (r *SegmentStorage) CountActiveKeys(segmentName string) (int64, error) {
	segmentKey := strings.Replace(redisSegment, "{segment}", segmentName, 1)
	return r.client.SCard(segmentKey)
}

// CountRemovedKeys count the numbers of removed keys in segmentName
func (r *SegmentStorage) CountRemovedKeys(segmentName string) (int64, error) {
	return 0, nil //not available on redis
}

// Keys returns the keys in segmentName
func (r *SegmentStorage) Keys(segmentName string) ([]dtos.SegmentKeyDTO, error) {
	keys := r.Get(segmentName).List()

	toReturn := make([]dtos.SegmentKeyDTO, 0, len(keys))
	for _, key := range keys {
		toReturn = append(toReturn, dtos.SegmentKeyDTO{Name: key.(string)})
	}
	return toReturn, nil
}

// RegisteredSegmentNames returns a list of strings
func (r *SegmentStorage) RegisteredSegmentNames() ([]string, error) {
	return r.client.SMembers(redisSegments)
}
