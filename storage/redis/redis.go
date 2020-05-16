package redis

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-toolkit/logging"
	"github.com/splitio/go-toolkit/redis"
	"github.com/splitio/go-toolkit/redis/helpers"
)

func parseTLSConfig(opt conf.RedisConfig) (*tls.Config, error) {
	if !opt.TLS {
		return nil, nil
	}

	if opt.SentinelReplication || opt.ClusterMode {
		return nil, errors.New("TLS encryption cannot be used with Sentinel replication or Cluster mode enabled")
	}

	cfg := tls.Config{}

	if opt.TLSServerName != "" {
		cfg.ServerName = opt.TLSServerName
	} else {
		cfg.ServerName = opt.Host
	}

	if len(opt.TLSCACertificates) > 0 {
		certPool := x509.NewCertPool()
		for _, cacert := range opt.TLSCACertificates {
			pemData, err := ioutil.ReadFile(cacert)
			if err != nil {
				return nil, err
			}
			ok := certPool.AppendCertsFromPEM(pemData)
			if !ok {
				return nil, fmt.Errorf("Couldn't add certificate %s to redis TLS configuration", cacert)
			}
		}
		cfg.RootCAs = certPool
	}

	cfg.InsecureSkipVerify = opt.TLSSkipNameValidation

	if opt.TLSClientKey != "" && opt.TLSClientCertificate != "" {
		certPair, err := tls.LoadX509KeyPair(
			opt.TLSClientCertificate,
			opt.TLSClientKey,
		)

		if err != nil {
			return nil, err
		}

		cfg.Certificates = []tls.Certificate{certPair}
	} else if opt.TLSClientKey != opt.TLSClientCertificate {
		// If they aren't both set, and they aren't equal, it means that only one is set, which is invalid.
		return nil, errors.New("You must provide either both client certificate and client private key, or none")
	}

	return &cfg, nil
}

// NewRedisClient returns a new Prefixed Redis Client
func NewRedisClient(config *conf.RedisConfig, logger logging.LoggerInterface) (*redis.PrefixedRedisClient, error) {
	prefix := config.Prefix

	tlsCfg, err := parseTLSConfig(*config)
	if err != nil {
		return nil, errors.New("Error in Redis TLS Configuration")
	}

	if config.SentinelReplication && config.ClusterMode {
		return nil, errors.New("Incompatible configuration of redis, Sentinel and Cluster cannot be enabled at the same time")
	}

	universalOptions := &redis.UniversalOptions{
		Password:     config.Password,
		DB:           config.Database,
		TLSConfig:    tlsCfg,
		MaxRetries:   config.MaxRetries,
		PoolSize:     config.PoolSize,
		DialTimeout:  time.Duration(config.DialTimeout) * time.Second,
		ReadTimeout:  time.Duration(config.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(config.WriteTimeout) * time.Second,
	}

	if config.SentinelReplication {
		if config.SentinelMaster == "" {
			return nil, errors.New("Missing redis sentinel master name")
		}

		if config.SentinelAddresses == "" {
			return nil, errors.New("Missing redis sentinels addresses")
		}

		universalOptions.MasterName = config.SentinelMaster
		universalOptions.Addrs = strings.Split(config.SentinelAddresses, ",")
	} else {
		if config.ClusterMode {
			if config.ClusterNodes == "" {
				return nil, errors.New("Missing redis cluster addresses")
			}

			var keyHashTag = "{SPLITIO}"

			if config.ClusterKeyHashTag != "" {
				keyHashTag = config.ClusterKeyHashTag
				if len(keyHashTag) < 3 ||
					string(keyHashTag[0]) != "{" ||
					string(keyHashTag[len(keyHashTag)-1]) != "}" ||
					strings.Count(keyHashTag, "{") != 1 ||
					strings.Count(keyHashTag, "}") != 1 {
					return nil, errors.New("keyHashTag is not valid")
				}
			}

			prefix = keyHashTag + prefix
			universalOptions.Addrs = strings.Split(config.ClusterNodes, ",")
		} else {
			universalOptions.Addrs = []string{fmt.Sprintf("%s:%d", config.Host, config.Port)}
		}
	}

	rClient, err := redis.NewClient(universalOptions)

	if err != nil {
		logger.Error(err.Error())
	}
	helpers.EnsureConnected(rClient)

	return redis.NewPrefixedRedisClient(rClient, config.Prefix)
}
