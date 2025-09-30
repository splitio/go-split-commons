package redis

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/splitio/go-split-commons/v7/dtos"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/splitio/go-toolkit/v5/redis"
)

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

func (r *RuleBasedSegmentStorage) ruleBasedSegment(ruleBased string) (*dtos.RuleBasedSegmentDTO, error) {
	keyToFetch := strings.Replace(KeyRuleBasedSegment, "{rbsegment}", ruleBased, 1)
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
func (r *RuleBasedSegmentStorage) GetRuleBasedSegmentByName(ruleBased string) *dtos.RuleBasedSegmentDTO {
	res, err := r.ruleBasedSegment(ruleBased)
	if err != nil {
		r.logger.Error(err.Error())
		return nil
	}

	return res
}
