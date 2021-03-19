package telemetry

import (
	"github.com/splitio/go-split-commons/v3/conf"
)

type InitSynchronizer interface {
	Record(cfg conf.InitConfig, timedUntilReady int64, factoryInstances map[string]int64, tags []string)
}
