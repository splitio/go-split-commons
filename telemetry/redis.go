package telemetry

import (
	"github.com/splitio/go-split-commons/v4/dtos"
	"github.com/splitio/go-split-commons/v4/storage"
	"github.com/splitio/go-toolkit/v5/datastructures/set"
	"github.com/splitio/go-toolkit/v5/logging"
)

// SynchronizerRedis struct
type SynchronizerRedis struct {
	storage storage.TelemetryConfigProducer
	logger  logging.LoggerInterface
}

// NewSynchronizerRedis constructor
func NewSynchronizerRedis(storage storage.TelemetryConfigProducer, logger logging.LoggerInterface) TelemetrySynchronizer {
	return &SynchronizerRedis{
		storage: storage,
		logger:  logger,
	}
}

// SynchronizeStats no-op
func (r *SynchronizerRedis) SynchronizeStats() error {
	// No-Op. Not required for redis. This will be implemented by Synchronizer.
	return nil
}

// SynchronizeConfig syncs config
func (r *SynchronizerRedis) SynchronizeConfig(cfg InitConfig, timedUntilReady int64, factoryInstances map[string]int64, tags []string) {
	err := r.storage.RecordConfigData(dtos.Config{
		OperationMode:      Consumer,
		Storage:            Redis,
		ActiveFactories:    int64(len(factoryInstances)),
		RedundantFactories: getRedudantActiveFactories(factoryInstances),
		Tags:               tags,
	})
	if err != nil {
		r.logger.Error("Could not log config data", err.Error())
	}
}

// SynchronizeUniqueKeys syncs unique keys
func (r *SynchronizerRedis) SynchronizeUniqueKeys(uniques map[string]*set.ThreadUnsafeSet) error {
	uniqueKeys := getUniqueKeysDto(uniques)

	err := r.storage.RecordUniqueKeys(uniqueKeys)
	if err != nil {
		r.logger.Error("Could not record the unique keys.", err.Error())
	}

	return nil
}
