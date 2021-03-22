package telemetry

import (
	"github.com/splitio/go-split-commons/v3/dtos"
	"github.com/splitio/go-split-commons/v3/storage"
	"github.com/splitio/go-toolkit/v4/logging"
)

type SynchronizerRedis struct {
	storage storage.TelemetryStorageProducer
	logger  logging.LoggerInterface
}

func NewSynchronizerRedis(storage storage.TelemetryStorageProducer, logger logging.LoggerInterface) TelemetrySynchronizer {
	return &SynchronizerRedis{
		storage: storage,
		logger:  logger,
	}
}

func (r *SynchronizerRedis) SynchronizeStats() error {
	panic("Not implemented")
}

func (r *SynchronizerRedis) SynchronizeInit(cfg InitConfig, timedUntilReady int64, factoryInstances map[string]int64, tags []string) {
	err := r.storage.RecordInitData(dtos.Init{
		OperationMode:      Consumer,
		Storage:            Redis,
		ActiveFactories:    int64(len(factoryInstances)),
		RedundantFactories: getRedudantActiveFactories(factoryInstances),
		Tags:               tags,
	})
	if err != nil {
		r.logger.Error("Could not log init data", err.Error())
	}
}
