package redis

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/splitio/go-split-commons/v4/dtos"
	"github.com/splitio/go-split-commons/v4/storage"
	"github.com/splitio/go-split-commons/v4/telemetry"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/splitio/go-toolkit/v5/redis"
)

const (
	sdkVersionKey  = "{sdkVersion}"
	machineIPKey   = "{machineIP}"
	machineNameKey = "{machineName}"
	methodKey      = "{method}"
	bucketIndexKey = "{bucket}"
)

// TelemetryStorage is a redis-based implementation of telemetry storage
type TelemetryStorage struct {
	client            *redis.PrefixedRedisClient
	exceptionTemplate string
	latencyTemplate   string
	logger            logging.LoggerInterface
	metadata          dtos.Metadata
}

// NewTelemetryStorage creates a new RedisTelemetryStorage and returns a reference to it
func NewTelemetryStorage(redisClient *redis.PrefixedRedisClient, logger logging.LoggerInterface, metadata dtos.Metadata) storage.TelemetryRedisProducer {
	replacer := strings.NewReplacer(sdkVersionKey, metadata.SDKVersion, machineNameKey, metadata.MachineName, machineIPKey, metadata.MachineIP)

	return &TelemetryStorage{
		client:            redisClient,
		exceptionTemplate: replacer.Replace(FieldException),
		latencyTemplate:   replacer.Replace(FieldLatency),
		logger:            logger,
		metadata:          metadata,
	}
}

// TELEMETRY STORAGE PRODUCER

// RecordConfigData push config into queue
func (t *TelemetryStorage) RecordConfigData(configData dtos.Config) error {
	jsonData, err := json.Marshal(dtos.TelemetryQueueObject{
		Metadata: t.metadata,
		Config:   configData,
	})
	if err != nil {
		t.logger.Error("Error encoding impression in json", err.Error())
	}

	inserted, errPush := t.client.RPush(KeyConfig, jsonData)
	if errPush != nil {
		t.logger.Error("Something were wrong pushing config data to redis", errPush)
		return errPush
	}

	// Checks if expiration needs to be set
	if inserted == 1 {
		t.logger.Debug("Proceeding to set expiration for: ", KeyConfig)
		result := t.client.Expire(KeyConfig, time.Duration(TTLConfig)*time.Second)
		if !result {
			t.logger.Error("Something were wrong setting expiration", errPush)
		}
	}
	return nil
}

// RecordLatency stores latency for method
func (t *TelemetryStorage) RecordLatency(method string, latency time.Duration) {
	bucket := telemetry.Bucket(latency.Milliseconds())
	field := strings.Replace(t.latencyTemplate, methodKey, method, 1)
	field = strings.Replace(field, bucketIndexKey, strconv.Itoa(bucket), 1)
	_, err := t.client.HIncrBy(KeyLatency, field, 1)
	if err != nil {
		t.logger.Error("Error recording in redis.", err.Error())
	}
}

// RecordException stores exceptions for method
func (t *TelemetryStorage) RecordException(method string) {
	field := strings.Replace(t.exceptionTemplate, methodKey, method, 1)
	_, err := t.client.HIncrBy(KeyException, field, 1)
	if err != nil {
		t.logger.Error("Error recording in redis.", err.Error())
	}
}

// RecordNonReadyUsage records non ready usage
func (t *TelemetryStorage) RecordNonReadyUsage() {
	// No-Op. Redis is implicitly ready and does not need to wait for anything. Tracking not required.
}

// RecordBURTimeout records bur timeodout
func (t *TelemetryStorage) RecordBURTimeout() {
	// No-Op. Redis is implicitly ready and does not need to block for anything. Tracking not required.
}
