package redis

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/splitio/go-split-commons/v3/dtos"
	"github.com/splitio/go-split-commons/v3/storage"
	"github.com/splitio/go-split-commons/v3/util"
	"github.com/splitio/go-toolkit/v4/logging"
	"github.com/splitio/go-toolkit/v4/redis"
)

const (
	sdkVersion  = "{sdkVersion}"
	machineIP   = "{machineIP}"
	machineName = "{machineName}"
	name        = "{method}"
	bucketName  = "{bucket}"
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
	replacer := strings.NewReplacer(sdkVersion, metadata.SDKVersion, machineName, metadata.MachineName, machineIP, metadata.MachineIP)
	exceptionTemplate := replacer.Replace(redisExceptionField)
	latencyTemplate := replacer.Replace(redisLatencyField)

	return &TelemetryStorage{
		client:            redisClient,
		exceptionTemplate: exceptionTemplate,
		latencyTemplate:   latencyTemplate,
		logger:            logger,
		metadata:          metadata,
	}
}

// TELEMETRY STORAGE PRODUCER

// RecordInitData push inits into queue
func (t *TelemetryStorage) RecordInitData(initData dtos.Init) error {
	jsonData, err := json.Marshal(dtos.TelemetryQueueObject{
		Metadata: t.metadata,
		Init:     initData,
	})
	if err != nil {
		t.logger.Error("Error encoding impression in json", err.Error())
	}

	inserted, errPush := t.client.RPush(redisInit, jsonData)
	if errPush != nil {
		t.logger.Error("Something were wrong pushing init data to redis", errPush)
		return errPush
	}

	// Checks if expiration needs to be set
	if inserted == 1 {
		t.logger.Debug("Proceeding to set expiration for: ", redisInit)
		result := t.client.Expire(redisInit, time.Duration(redisInitTTL)*time.Second)
		if !result {
			t.logger.Error("Something were wrong setting expiration", errPush)
		}
	}
	return nil
}

// RecordLatency stores latency for method
func (t *TelemetryStorage) RecordLatency(method string, latency int64) {
	bucket := util.Bucket(latency)
	field := strings.Replace(t.latencyTemplate, name, method, 1)
	field = strings.Replace(field, bucketName, fmt.Sprintf("%d", bucket), 1)
	_, err := t.client.HIncrBy(redisLatency, field, 1)
	if err != nil {
		t.logger.Error("Error recording in redis.", err.Error())
	}
}

// RecordException stores exceptions for method
func (t *TelemetryStorage) RecordException(method string) {
	field := strings.Replace(t.exceptionTemplate, name, method, 1)
	_, err := t.client.HIncrBy(redisException, field, 1)
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
