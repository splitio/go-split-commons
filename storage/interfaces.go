package storage

import (
	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-toolkit/datastructures/set"
)

// SplitStorageProducer should be implemented by structs that offer writing splits in storage
type SplitStorageProducer interface {
	PutMany(splits []dtos.SplitDTO, changeNumber int64) // Maybe Move Put to this....Doesn't exist in spec
	Remove(splitname string)
	ChangeNumber() (int64, error)
	SetChangeNumber(changeNumber int64) error
	Clear()
}

// SplitStorageConsumer should be implemented by structs that offer reading splits from storage
type SplitStorageConsumer interface {
	All() []dtos.SplitDTO
	FetchMany(splitNames []string) map[string]*dtos.SplitDTO
	Split(splitName string) *dtos.SplitDTO
	SplitNames() []string
	SegmentNames() *set.ThreadUnsafeSet // Not in Spec
	TrafficTypeExists(trafficType string) bool
}

// SegmentStorageProducer interface should be implemented by all structs that offer writing segments
type SegmentStorageProducer interface {
	ChangeNumber(segmentName string) (int64, error)
	Clear()
	Update(name string, toAdd *set.ThreadUnsafeSet, toRemove *set.ThreadUnsafeSet, changeNumber int64) error
	SetChangeNumber(segmentName string, till int64) error
}

// SegmentStorageConsumer interface should be implemented by all structs that ofer reading segments
type SegmentStorageConsumer interface {
	Keys(segmentName string) *set.ThreadUnsafeSet
	// MISSING IsInSegment(name, key)
}

// ImpressionStorageProducer interface should be impemented by structs that accept incoming impressions
type ImpressionStorageProducer interface {
	LogImpressions(impressions []dtos.Impression) error
}

// ImpressionStorageConsumer interface should be implemented by structs that offer popping impressions
type ImpressionStorageConsumer interface {
	Empty() bool
	PopN(n int64) ([]dtos.Impression, error)
}

// MetricsStorageProducer interface should be impemented by structs that accept incoming metrics
type MetricsStorageProducer interface {
	PutGauge(key string, gauge float64)
	IncLatency(metricName string, index int)
	IncCounter(key string)
}

// MetricsStorageConsumer interface should be implemented by structs that offer popping metrics
type MetricsStorageConsumer interface {
	PopGauges() []dtos.GaugeDTO
	PopLatencies() []dtos.LatenciesDTO
	PopCounters() []dtos.CounterDTO
}

// EventStorageProducer interface should be implemented by structs that accept incoming events
type EventStorageProducer interface {
	Push(event dtos.EventDTO, size int) error
}

// EventStorageConsumer interface should be implemented by structs that offer popping impressions
type EventStorageConsumer interface {
	Count() int64
	Empty() bool
	PopN(n int64) ([]dtos.EventDTO, error)
	PopNWithMetadata(n int64) ([]dtos.QueueStoredEventDTO, error)
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

// MetricsStorage wraps consumer and producer interfaces
type MetricsStorage interface {
	MetricsStorageConsumer
	MetricsStorageProducer
}

// EventsStorage wraps consumer and producer interfaces
type EventsStorage interface {
	EventStorageConsumer
	EventStorageProducer
}
