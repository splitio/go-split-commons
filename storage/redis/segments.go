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
