package dtos

// EventDTO struct mapping events json
type EventDTO struct {
	Key             string                 `json:"key"`
	TrafficTypeName string                 `json:"trafficTypeName"`
	EventTypeID     string                 `json:"eventTypeId"`
	Value           interface{}            `json:"value"`
	Timestamp       int64                  `json:"timestamp"`
	Properties      map[string]interface{} `json:"properties,omitempty"`
}

// QueueStoredEventDTO maps the stored JSON object in redis by SDKs
type QueueStoredEventDTO struct {
	Metadata Metadata `json:"m"`
	Event    EventDTO `json:"e"`
}
