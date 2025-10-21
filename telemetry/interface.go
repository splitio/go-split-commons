package telemetry

import "github.com/splitio/go-split-commons/v8/dtos"

// TelemetrySynchronizer interface
type TelemetrySynchronizer interface {
	SynchronizeConfig(cfg InitConfig, timedUntilReady int64, factoryInstances map[string]int64, tags []string)
	SynchronizeStats() error
	SynchronizeUniqueKeys(uniques dtos.Uniques) error
}
