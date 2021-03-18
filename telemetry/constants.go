package telemetry

const (
	// Treatment getTreatment
	Treatment = "treatment"
	// Treatments getTreatments
	Treatments = "treatments"
	// TreatmentWithConfig getTreatmentWithConfig
	TreatmentWithConfig = "treatmentWithConfig"
	// TreatmentsWithConfig getTreatmentsWithConfig
	TreatmentsWithConfig = "treatmentsWithConfig"
	// Track track
	Track = "track"
)

const (
	// SplitSync splitChanges
	SplitSync = iota
	// SegmentSync segmentChanges
	SegmentSync
	// ImpressionSync impressions
	ImpressionSync
	// EventSync events
	EventSync
	// TelemetrySync telemetry
	TelemetrySync
	// TokenSync auth
	TokenSync
)

const (
	// ImpressionsDropped dropped
	ImpressionsDropped = iota
	// ImpressionsDeduped deduped
	ImpressionsDeduped
	// ImpressionsQueued queued
	ImpressionsQueued
)

const (
	// EventsDropped dropped
	EventsDropped = iota
	// EventsQueued queued
	EventsQueued
)

const (
	// LatencyBucketCount Max buckets
	LatencyBucketCount = 23
	// MaxStreamingEvents Max streaming events allowed
	MaxStreamingEvents = 20
	// MaxTags Max tags
	MaxTags = 10
)

const (
	EventTypeSSEConnectionEstablished = iota * 10
	EventTypeOccupancyPri
	EventTypeOccupancySec
	EventTypeStreamingStatus
	EventTypeConnectionError
	EventTypeTokenRefresh
	EventTypeAblyError
	EventTypeSyncMode
)

const (
	StreamingDisabled = iota
	StreamingEnabled
	StreamingPaused
)

const (
	Requested = iota
	NonRequested
)

const (
	Streaming = iota
	Polling
)

const (
	Standalone = iota
	Consumer
)
