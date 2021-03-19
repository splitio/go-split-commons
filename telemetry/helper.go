package telemetry

import (
	"time"

	"github.com/splitio/go-split-commons/v3/dtos"
)

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
