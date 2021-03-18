package redis

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/splitio/go-split-commons/v3/dtos"
	"github.com/splitio/go-split-commons/v3/storage"
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
func NewTelemetryStorage(redisClient *redis.PrefixedRedisClient, logger logging.LoggerInterface, metadata dtos.Metadata) storage.TelemetryStorage {
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
		if result == false {
			t.logger.Error("Something were wrong setting expiration", errPush)
		}
	}
	return nil
}

// RecordLatency stores latency for method
func (t *TelemetryStorage) RecordLatency(method string, bucket int) {
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

// RecordImpressionsStats records impressions by type
func (t *TelemetryStorage) RecordImpressionsStats(dataType int, count int64) {
	panic("Not implemented for redis")
}

// RecordEventsStats recirds events by type
func (t *TelemetryStorage) RecordEventsStats(dataType int, count int64) {
	panic("Not implemented for redis")
}

// RecordSuccessfulSync records sync for resource
func (t *TelemetryStorage) RecordSuccessfulSync(resource int, timestamp int64) {
	panic("Not implemented for redis")
}

// RecordSyncError records http error
func (t *TelemetryStorage) RecordSyncError(resource int, status int) {
	panic("Not implemented for redis")
}

// RecordSyncLatency records http error
func (t *TelemetryStorage) RecordSyncLatency(resource int, bucket int) {
	panic("Not implemented for redis")
}

// RecordAuthRejections records auth rejections
func (t *TelemetryStorage) RecordAuthRejections() {
	panic("Not implemented for redis")
}

// RecordTokenRefreshes records token
func (t *TelemetryStorage) RecordTokenRefreshes() {
	panic("Not implemented for redis")
}

// RecordStreamingEvent appends new streaming event
func (t *TelemetryStorage) RecordStreamingEvent(event dtos.StreamingEvent) {
	panic("Not implemented for redis")
}

// AddTag adds particular tag
func (t *TelemetryStorage) AddTag(tag string) {
	panic("Not implemented for redis")
}

// RecordSessionLength records session length
func (t *TelemetryStorage) RecordSessionLength(session int64) {
	panic("Not implemented for redis")
}

// RecordNonReadyUsage records non ready usage
func (t *TelemetryStorage) RecordNonReadyUsage() {
	panic("Not implemented for redis")
}

// RecordBURTimeout records bur timeodout
func (t *TelemetryStorage) RecordBURTimeout() {
	panic("Not implemented for redis")
}

// TELEMETRY STORAGE CONSUMER

// PopLatencies gets and clears method latencies
func (t *TelemetryStorage) PopLatencies() dtos.MethodLatencies {
	panic("Not implemented for redis")
}

// PopExceptions gets and clears method exceptions
func (t *TelemetryStorage) PopExceptions() dtos.MethodExceptions {
	panic("Not implemented for redis")
}

// GetImpressionsStats gets impressions by type
func (t *TelemetryStorage) GetImpressionsStats(dataType int) int64 {
	panic("Not implemented for redis")
}

// GetEventsStats gets events by type
func (t *TelemetryStorage) GetEventsStats(dataType int) int64 {
	panic("Not implemented for redis")
}

// GetLastSynchronization gets last synchronization stats for fetchers and recorders
func (t *TelemetryStorage) GetLastSynchronization() dtos.LastSynchronization {
	panic("Not implemented for redis")
}

// PopHTTPErrors gets http errors
func (t *TelemetryStorage) PopHTTPErrors() dtos.HTTPErrors {
	panic("Not implemented for redis")
}

// PopHTTPLatencies gets http latencies
func (t *TelemetryStorage) PopHTTPLatencies() dtos.HTTPLatencies {
	panic("Not implemented for redis")
}

// PopAuthRejections gets total amount of auth rejections
func (t *TelemetryStorage) PopAuthRejections() int64 {
	panic("Not implemented for redis")
}

// PopTokenRefreshes gets total amount of token refreshes
func (t *TelemetryStorage) PopTokenRefreshes() int64 {
	panic("Not implemented for redis")
}

// PopStreamingEvents gets streamingEvents data
func (t *TelemetryStorage) PopStreamingEvents() []dtos.StreamingEvent {
	panic("Not implemented for redis")
}

// PopTags gets total amount of tags
func (t *TelemetryStorage) PopTags() []string {
	panic("Not implemented for redis")
}

// GetSessionLength gets session duration
func (t *TelemetryStorage) GetSessionLength() int64 {
	panic("Not implemented for redis")
}

// GetNonReadyUsages gets non usages on ready
func (t *TelemetryStorage) GetNonReadyUsages() int64 {
	panic("Not implemented for redis")
}

// GetBURTimeouts gets timedouts data
func (t *TelemetryStorage) GetBURTimeouts() int64 {
	panic("Not implemented for redis")
}
