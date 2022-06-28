package telemetry

import "github.com/splitio/go-toolkit/v5/datastructures/set"

// TelemetrySynchronizer interface
type TelemetrySynchronizer interface {
	SynchronizeConfig(cfg InitConfig, timedUntilReady int64, factoryInstances map[string]int64, tags []string)
	SynchronizeStats() error
	SynchronizeUniqueKeys(uniques map[string]*set.ThreadUnsafeSet) error
}
