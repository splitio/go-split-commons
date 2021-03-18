package storage

import (
	"github.com/splitio/go-split-commons/v3/dtos"
	"github.com/splitio/go-toolkit/v4/datastructures/set"
)

// SplitStorageProducer should be implemented by structs that offer writing splits in storage
type SplitStorageProducer interface {
	KillLocally(splitName string, defaultTreatment string, changeNumber int64)
	PutMany(splits []dtos.SplitDTO, changeNumber int64)
	Remove(splitName string)
	SetChangeNumber(changeNumber int64) error
}

// SplitStorageConsumer should be implemented by structs that offer reading splits from storage
type SplitStorageConsumer interface {
	All() []dtos.SplitDTO
	ChangeNumber() (int64, error)
	FetchMany(splitNames []string) map[string]*dtos.SplitDTO
	SegmentNames() *set.ThreadUnsafeSet // Not in Spec
	Split(splitName string) *dtos.SplitDTO
	SplitNames() []string
	TrafficTypeExists(trafficType string) bool
}

// SegmentStorageProducer interface should be implemented by all structs that offer writing segments
type SegmentStorageProducer interface {
	Update(name string, toAdd *set.ThreadUnsafeSet, toRemove *set.ThreadUnsafeSet, changeNumber int64) error
	SetChangeNumber(segmentName string, till int64) error
}

// SegmentStorageConsumer interface should be implemented by all structs that ofer reading segments
type SegmentStorageConsumer interface {
	ChangeNumber(segmentName string) (int64, error)
	CountRemovedKeys(segmentName string) int64
	Keys(segmentName string) *set.ThreadUnsafeSet
	SegmentContainsKey(segmentName string, key string) (bool, error)
}

// ImpressionStorageProducer interface should be impemented by structs that accept incoming impressions
type ImpressionStorageProducer interface {
	LogImpressions(impressions []dtos.Impression) error
}

// ImpressionStorageConsumer interface should be implemented by structs that offer popping impressions
type ImpressionStorageConsumer interface {
	Count() int64
	Drop(size *int64) error
	Empty() bool
	PopN(n int64) ([]dtos.Impression, error)
	PopNWithMetadata(n int64) ([]dtos.ImpressionQueueObject, error)
}

// EventStorageProducer interface should be implemented by structs that accept incoming events
type EventStorageProducer interface {
	Push(event dtos.EventDTO, size int) error
}

// EventStorageConsumer interface should be implemented by structs that offer popping impressions
type EventStorageConsumer interface {
	Count() int64
	Drop(size *int64) error
	Empty() bool
	PopN(n int64) ([]dtos.EventDTO, error)
	PopNWithMetadata(n int64) ([]dtos.QueueStoredEventDTO, error)
}

// TelemetryRedisProducer interface should be implemented by struct that accepts incoming telemetry in redis
type TelemetryRedisProducer interface {
	TelemetryConfigProducer
	TelemetryEvaluationProducer
}

// TelemetryStorageProducer interface should be implemented by structs that accept incoming telemetry for memory
type TelemetryStorageProducer interface {
	TelemetryEvaluationProducer
	TelemetryImpressionProducer
	TelemetryEventProducer
	TelemetrySynchronizationProducer
	TelemetryHTTPProducer
	TelemetryPushProducer
	TelemetryStreamingProducer
	TelemetryMiscProducer
	TelemetrySDKInfoProducer
}

// TelemetryConfigProducer interface for init data
type TelemetryConfigProducer interface {
	RecordInitData(initData dtos.Init) error
}

// TelemetryEvaluationProducer for evaluation
type TelemetryEvaluationProducer interface {
	RecordLatency(method string, bucket int)
	RecordException(method string)
}

// TelemetryImpressionProducer for impressions
type TelemetryImpressionProducer interface {
	RecordImpressionsStats(dataType int, count int64)
}

// TelemetryEventProducer for events
type TelemetryEventProducer interface {
	RecordEventsStats(dataType int, count int64)
}

// TelemetrySynchronizationProducer for sync
type TelemetrySynchronizationProducer interface {
	RecordSuccessfulSync(resource int, time int64)
}

// TelemetryHTTPProducer for http
type TelemetryHTTPProducer interface {
	RecordSyncError(resource int, status int)
	RecordSyncLatency(resource int, bucket int)
}

// TelemetryPushProducer for push
type TelemetryPushProducer interface {
	RecordAuthRejections()
	RecordTokenRefreshes()
}

// TelemetryStreamingProducer for streaming
type TelemetryStreamingProducer interface {
	RecordStreamingEvent(streamingEvent dtos.StreamingEvent)
}

// TelemetryMiscProducer for misc
type TelemetryMiscProducer interface {
	AddTag(tag string)
}

// TelemetrySDKInfoProducer for sdk
type TelemetrySDKInfoProducer interface {
	RecordSessionLength(session int64)
	RecordNonReadyUsage()
	RecordBURTimeout()
}

// TelemetryStorageConsumer interface should be implemented by structs that offer popping telemetry
type TelemetryStorageConsumer interface {
	PopLatencies() dtos.MethodLatencies
	PopExceptions() dtos.MethodExceptions
	GetImpressionsStats(dataType int) int64
	GetEventsStats(dataType int) int64
	GetLastSynchronization() dtos.LastSynchronization
	PopHTTPErrors() dtos.HTTPErrors
	PopHTTPLatencies() dtos.HTTPLatencies
	PopAuthRejections() int64
	PopTokenRefreshes() int64
	PopStreamingEvents() []dtos.StreamingEvent
	PopTags() []string
	GetSessionLength() int64
	GetNonReadyUsages() int64
	GetBURTimeouts() int64
}

// --- Wide Interfaces

// SplitStorage wraps consumer & producer interfaces
type SplitStorage interface {
	SplitStorageProducer
	SplitStorageConsumer
}

// SegmentStorage wraps consumer and producer interfaces
type SegmentStorage interface {
	SegmentStorageProducer
	SegmentStorageConsumer
}

// ImpressionStorage wraps consumer & producer interfaces
type ImpressionStorage interface {
	ImpressionStorageConsumer
	ImpressionStorageProducer
}

// EventsStorage wraps consumer and producer interfaces
type EventsStorage interface {
	EventStorageConsumer
	EventStorageProducer
}

// TelemetryStorage wraps consumer and producer interfaces
type TelemetryStorage interface {
	TelemetryStorageConsumer
	TelemetryStorageProducer
}
