package telemetry

import "github.com/splitio/go-toolkit/v5/datastructures/set"

type NoOp struct{}

func (n *NoOp) SynchronizeConfig(cfg InitConfig, timedUntilReady int64, factoryInstances map[string]int64, tags []string) {
}

func (n *NoOp) SynchronizeStats() error {
	return nil
}

func (n *NoOp) SynchronizeUniqueKeys(uniques map[string]*set.ThreadUnsafeSet) error {
	return nil
}
