package telemetry

type NoOp struct{}

func (n *NoOp) SynchronizeConfig(cfg InitConfig, timedUntilReady int64, factoryInstances map[string]int64, tags []string) {
}

func (n *NoOp) SynchronizeStats() error {
	return nil
}

func (n *NoOp) SynchronizeUniqueKeys(bulkSize int64) error {
	return nil
}

var _ TelemetrySynchronizer = (*NoOp)(nil)
