package util

import "github.com/splitio/go-split-commons/conf"

// ShouldAddPreviousTime returns if previous time should be set up or not depending on operationMode
func ShouldAddPreviousTime(managerConfig conf.ManagerConfig) bool {
	switch managerConfig.OperationMode {
	case conf.Standalone:
		return true
	default:
		return false
	}
}

// ShouldBeOptimized returns if should dedupe impressions or not depending on configs
func ShouldBeOptimized(managerConfig conf.ManagerConfig) bool {
	if !ShouldAddPreviousTime(managerConfig) {
		return false
	}
	if managerConfig.ImpressionsMode == conf.Optimized {
		return true
	}
	return false
}