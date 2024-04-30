package service

import (
	"github.com/splitio/go-split-commons/v5/dtos"
)

// AuthClient inteface to be implemneted by AuthClient
type AuthClient interface {
	Authenticate() (*dtos.Token, error)
}

// SplitFetcher interface to be implemented by Split Fetchers
type SplitFetcher interface {
	Fetch(fetchOptions *FlagRequestParams) (*dtos.SplitChangesDTO, error)
}

// SegmentFetcher interface to be implemented by Split Fetchers
type SegmentFetcher interface {
	Fetch(name string, fetchOptions *SegmentRequestParams) (*dtos.SegmentChangesDTO, error)
}

// ImpressionsRecorder interface to be implemented by Impressions loggers
type ImpressionsRecorder interface {
	Record(impressions []dtos.ImpressionsDTO, metadata dtos.Metadata, extraHeaders map[string]string) error
	RecordImpressionsCount(pf dtos.ImpressionsCountDTO, metadata dtos.Metadata) error
}

// TelemetryRecorder interface to be implemented by Telemetry loggers
type TelemetryRecorder interface {
	RecordConfig(config dtos.Config, metadata dtos.Metadata) error
	RecordStats(stats dtos.Stats, metadata dtos.Metadata) error
	RecordUniqueKeys(uniques dtos.Uniques, metadata dtos.Metadata) error
}

// EventsRecorder interface to post events
type EventsRecorder interface {
	Record(events []dtos.EventDTO, metadata dtos.Metadata) error
}
