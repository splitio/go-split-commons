package redis

import (
	"errors"
	"strings"

	"github.com/splitio/go-toolkit/logging"
	"github.com/splitio/go-toolkit/redis"
)

// ErrorHashNotPresent error
const ErrorHashNotPresent = "hash-not-present"

const clearAllSCriptTemplate = `
	local toDelete = redis.call('KEYS', '{KEY_NAMESPACE}*')
	local count = 0
	for _, key in ipairs(toDelete) do
	    redis.call('DEL', key)
	    count = count + 1
	end
	return count
`

// MiscStorage is a redis-based provides methods to handle the synchronizer's initialization procedure
type MiscStorage struct {
	client *redis.PrefixedRedisClient
	logger logging.LoggerInterface
}

// NewMiscStorage creates a new MiscStorage and returns a reference to it
func NewMiscStorage(redisClient *redis.PrefixedRedisClient, logger logging.LoggerInterface) *MiscStorage {
	return &MiscStorage{
		client: redisClient,
		logger: logger,
	}
}

// GetApikeyHash gets
func (m *MiscStorage) GetApikeyHash() (string, error) {
	res, err := m.client.Get(redisHash)
	if err != nil && err.Error() == "redis: nil" {
		return "", errors.New(ErrorHashNotPresent)
	}
	return res, err
}

// SetApikeyHash sets
func (m *MiscStorage) SetApikeyHash(newApikeyHash string) error {
	return m.client.Set(redisHash, newApikeyHash, 0)
}

// ClearAll clears
func (m *MiscStorage) ClearAll() error {
	luaCMD := strings.Replace(clearAllSCriptTemplate, "{KEY_NAMESPACE}", m.client.Prefix+".SPLITIO", 1)
	return m.client.Eval(luaCMD, nil, 0)
}
