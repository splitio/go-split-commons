package telemetry

import (
	"os"
	"strings"

	"github.com/splitio/go-split-commons/v3/conf"
	"github.com/splitio/go-split-commons/v3/dtos"
	"github.com/splitio/go-split-commons/v3/service"
	"github.com/splitio/go-split-commons/v3/storage"
	"github.com/splitio/go-toolkit/v4/logging"
)

func getURLOverrides(cfg conf.AdvancedConfig) dtos.URLOverrides {
	defaults := conf.GetDefaultAdvancedConfig()
	return dtos.URLOverrides{
		Sdk:       cfg.SdkURL != defaults.SdkURL,
		Events:    cfg.EventsURL != defaults.EventsURL,
		Auth:      cfg.AuthServiceURL != defaults.AuthServiceURL,
		Stream:    cfg.StreamingServiceURL != defaults.StreamingServiceURL,
		Telemetry: cfg.TelemetryServiceURL != defaults.TelemetryServiceURL,
	}
}

func getRedudantActiveFactories(factoryInstances map[string]int64) int64 {
	var toReturn int64 = 0
	for _, instances := range factoryInstances {
		toReturn = toReturn + instances - 1
	}
	return toReturn
}

type RecorderRedis struct {
	storage storage.TelemetryStorage
	logger  logging.LoggerInterface
}

func NewSenderRedis(storage storage.TelemetryStorage, logger logging.LoggerInterface) InitSynchronizer {
	return &RecorderRedis{
		storage: storage,
		logger:  logger,
	}
}

func (r *RecorderRedis) Record(cfg conf.InitConfig, timedUntilReady int64, factoryInstances map[string]int64, tags []string) {
	err := r.storage.RecordInitData(dtos.Init{
		OperationMode:      Consumer,
		Storage:            redis,
		ActiveFactories:    int64(len(factoryInstances)),
		RedundantFactories: getRedudantActiveFactories(factoryInstances),
		Tags:               tags,
	})
	if err != nil {
		r.logger.Error("Could not log init data", err.Error())
	}
}

type RecorderInMemory struct {
	facade   FacadeConsumer
	recorder service.TelemetryRecorder
	metadata dtos.Metadata
	logger   logging.LoggerInterface
}

func NewSenderInMemory(facade FacadeConsumer, recorder service.TelemetryRecorder, logger logging.LoggerInterface, metadata dtos.Metadata) InitSynchronizer {
	return &RecorderInMemory{
		facade:   facade,
		recorder: recorder,
		metadata: metadata,
		logger:   logger,
	}
}

func (r *RecorderInMemory) Record(cfg conf.InitConfig, timedUntilReady int64, factoryInstances map[string]int64, tags []string) {
	urlOverrides := getURLOverrides(cfg.AdvancedConfig)

	impressionsMode := impressionsModeOptimized
	if cfg.ManagerConfig.ImpressionsMode == conf.ImpressionsModeDebug {
		impressionsMode = impressionsModeDebug
	}

	err := r.recorder.RecordInit(dtos.Init{
		OperationMode:      Standalone,
		Storage:            memory,
		ActiveFactories:    int64(len(factoryInstances)),
		RedundantFactories: getRedudantActiveFactories(factoryInstances),
		Tags:               tags,
		StreamingEnabled:   cfg.AdvancedConfig.StreamingEnabled,
		Rates: &dtos.Rates{
			Splits:      int64(cfg.TaskPeriods.SplitSync),
			Segments:    int64(cfg.TaskPeriods.SegmentSync),
			Impressions: int64(cfg.TaskPeriods.ImpressionSync),
			Events:      int64(cfg.TaskPeriods.EventsSync),
			Telemetry:   int64(cfg.TaskPeriods.CounterSync), // It should be TelemetrySync after refactor in go
		},
		URLOverrides:               &urlOverrides,
		ImpressionsQueueSize:       int64(cfg.AdvancedConfig.ImpressionsQueueSize),
		EventsQueueSize:            int64(cfg.AdvancedConfig.EventsQueueSize),
		ImpressionsMode:            impressionsMode,
		ImpressionsListenerEnabled: cfg.ManagerConfig.ListenerEnabled,
		HTTPProxyDetected:          len(strings.TrimSpace(os.Getenv("HTTP_PROXY"))) > 0,
		TimeUntilReady:             timedUntilReady,
		BurTimeouts:                r.facade.GetBURTimeouts(),
		NonReadyUsages:             r.facade.GetNonReadyUsages(),
	}, r.metadata)
	if err != nil {
		r.logger.Error("Could not log init data", err.Error())
	}
}
