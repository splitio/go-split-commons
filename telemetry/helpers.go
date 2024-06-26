package telemetry

import (
	"time"

	"github.com/splitio/go-split-commons/v6/conf"
	"github.com/splitio/go-split-commons/v6/dtos"
)

// GetStreamingEvent get streaming event
func GetStreamingEvent(eventType int, data int64) *dtos.StreamingEvent {
	switch eventType {
	case EventTypeSSEConnectionEstablished, EventTypeOccupancyPri,
		EventTypeOccupancySec, EventTypeStreamingStatus,
		EventTypeConnectionError, EventTypeTokenRefresh,
		EventTypeAblyError, EventTypeSyncMode:
		return &dtos.StreamingEvent{
			Type:      eventType,
			Data:      data,
			Timestamp: time.Now().UTC().Unix(),
		}
	}
	return nil
}

func getImpressionMode(cfg InitConfig) int {
	switch cfg.ImpressionsMode {
	case conf.ImpressionsModeDebug:
		return ImpressionsModeDebug
	case conf.ImpressionsModeNone:
		return ImpressionsModeNone
	default:
		return ImpressionsModeOptimized
	}
}

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
