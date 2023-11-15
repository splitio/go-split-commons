package telemetry

import (
	"github.com/splitio/go-split-commons/v5/conf"
)

const (
	// Treatment getTreatment
	Treatment = "treatment"
	// Treatments getTreatments
	Treatments = "treatments"
	// TreatmentWithConfig getTreatmentWithConfig
	TreatmentWithConfig = "treatmentWithConfig"
	// TreatmentsWithConfig getTreatmentsWithConfig
	TreatmentsWithConfig = "treatmentsWithConfig"
	// TreatmentsByFlagSet getTreatmentsByFlagSet
	TreatmentsByFlagSet = "treatmentsByFlagSet"
	// TreatmentsByFlagSets getTreatmentsByFlagSets
	TreatmentsByFlagSets = "treatmentsByFlagSets"
	// TreatmentsWithConfigByFlagSet getTreatmentsWithConfigByFlagSet
	TreatmentsWithConfigByFlagSet = "treatmentsWithConfigByFlagSet"
	// TreatmentsWithConfigByFlagSets getTreatmentsWithConfigByFlagSets
	TreatmentsWithConfigByFlagSets = "treatmentsWithConfigByFlagSets"
	// Track track
	Track = "track"
)

// ParseMethodFromRedisHash parses the method in a latency/exception hash from redis and returns it's normalized version, or `ok` set to false
func ParseMethodFromRedisHash(method string) (normalized string, ok bool) {
	switch method {
	case "getTreatment", "get_treatment", "treatment", "Treatment":
		return Treatment, true
	case "getTreatments", "get_treatments", "treatments", "Treatments":
		return Treatments, true
	case "getTreatmentWithConfig", "get_treatment_with_config", "treatment_with_config", "treatmentWithConfig", "TreatmentWithConfig":
		return TreatmentWithConfig, true
	case "getTreatmentsWithConfig", "get_treatments_with_config", "treatments_with_config", "treatmentsWithConfig", "TreatmentsWithConfig":
		return TreatmentsWithConfig, true
	case "getTreatmentsByFlagSet", "get_treatments_by_flag_set", "treatmentsByFlagSet", "TreatmentsByFlagSet":
		return TreatmentsByFlagSet, true
	case "getTreatmentsByFlagSets", "get_treatments_by_flag_sets", "treatmentsByFlagSets", "TreatmentsByFlagSets":
		return TreatmentsByFlagSets, true
	case "getTreatmentsWithConfigByFlagSet", "get_treatments_with_config_by_flag_set", "treatmentsWithConfigByFlagSet", "TreatmentsWithConfigByFlagSet":
		return TreatmentsWithConfigByFlagSet, true
	case "getTreatmentsWithConfigByFlagSets", "get_treatments_with_config_by_flag_sets", "treatmentsWithConfigByFlagSets", "TreatmentsWithConfigByFlagSets":
		return TreatmentsWithConfigByFlagSets, true
	case "track", "Track":
		return Track, true
	default:
		return "", false
	}
}

// IsMethodValid returs true if the supplied method name is valid
func IsMethodValid(method *string) bool {
	switch *method {
	case "getTreatment", "get_treatment", "treatment", "Treatment":
	case "getTreatments", "get_treatments", "treatments", "Treatments":
	case "getTreatmentWithConfig", "get_treatment_with_config", "treatmentWithConfig", "TreatmentWithWconfig":
	case "getTreatmentsWithConfig", "get_treatments_with_config", "treatmentsWithConfig", "TreatmentsWithWconfig":
	case "getTreatmentsByFlagSet", "get_treatments_by_flag_set", "treatmentsByFlagSet", "TreatmentsByFlagSet":
	case "getTreatmentsByFlagSets", "get_treatments_by_flag_sets", "treatmentsByFlagSets", "TreatmentsByFlagSets":
	case "getTreatmentsWithConfigByFlagSet", "get_treatments_with_config_by_flag_set", "treatmentsWithConfigByFlagSet", "TreatmentsWithConfigByFlagSet":
	case "getTreatmentsWithConfigByFlagSets", "get_treatments_with_config_by_flag_sets", "treatmentsWithConfigByFlagSets", "TreatmentsWithConfigByFlagSets":
	case "track", "Track":
	default:
		return false
	}
	return true
}

const (
	// SplitSync splitChanges
	SplitSync = iota
	// SegmentSync segmentChanges
	SegmentSync
	// ImpressionSync impressions
	ImpressionSync
	// ImpressionCountSync impressionsCount
	ImpressionCountSync
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
	Producer
)

const (
	ImpressionsModeOptimized = iota
	ImpressionsModeDebug
	ImpressionsModeNone
)

const (
	Redis  = "redis"
	Memory = "memory"
)

const (
	SplitUpdate = iota
)

// InitConfig involves entire config for init
type InitConfig struct {
	AdvancedConfig  conf.AdvancedConfig
	TaskPeriods     conf.TaskPeriods
	ImpressionsMode string
	ListenerEnabled bool
	FlagSetsTotal   int64
	FlagSetsInvalid int64
}
