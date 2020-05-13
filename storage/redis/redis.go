package redis

import (
	"fmt"

	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-toolkit/logging"
	"github.com/splitio/go-toolkit/redis"
	"github.com/splitio/go-toolkit/redis/helpers"
)

// NewRedisClient returns a new Prefixed Redis Client
func NewRedisClient(config *conf.RedisConfig, logger logging.LoggerInterface) (*redis.PrefixedRedisClient, error) {
	rClient, err := redis.NewClient(&redis.UniversalOptions{
		Addrs:     []string{fmt.Sprintf("%s:%d", config.Host, config.Port)},
		Password:  config.Password,
		DB:        config.Database,
		TLSConfig: config.TLSConfig,
	})

	if err != nil {
		logger.Error(err.Error())
	}
	helpers.EnsureConnected(rClient)

	return redis.NewPrefixedRedisClient(rClient, config.Prefix)
}
