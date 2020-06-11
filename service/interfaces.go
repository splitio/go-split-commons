package service

import (
	"github.com/splitio/go-split-commons/dtos"
)

// SplitFetcher interface to be implemented by Split Fetchers
type SplitFetcher interface {
	Fetch(changeNumber int64) (*dtos.SplitChangesDTO, error)
}

// SegmentFetcher interface to be implemented by Split Fetchers
type SegmentFetcher interface {
	Fetch(name string, changeNumber int64) (*dtos.SegmentChangesDTO, error)
}

// ImpressionsRecorder interface to be implemented by Impressions loggers
type ImpressionsRecorder interface {
	Record(impressions []dtos.Impression, metadata dtos.Metadata) error
}

// MetricsRecorder interface to be implemented by Metrics loggers
type MetricsRecorder interface {
	RecordLatencies(latencies []dtos.LatenciesDTO, metadata dtos.Metadata) error
	RecordCounters(counters []dtos.CounterDTO, metadata dtos.Metadata) error
	RecordGauge(gauge dtos.GaugeDTO, metadata dtos.Metadata) error
}

// EventsRecorder interface to post events
type EventsRecorder interface {
	Record(events []dtos.EventDTO, metadata dtos.Metadata) error
}
