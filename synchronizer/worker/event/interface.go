package event

const (
	postEventsLatencies        = "events.time"
	postEventsLatenciesBackend = "backend::/api/events/bulk" // @TODO add local
	postEventsCounters         = "events.status.{status}"
	postEventsLocalCounters    = "backend::request.{status}" // @TODO add local
)

// EventRecorder interface
type EventRecorder interface {
	SynchronizeEvents(bulkSize int64) error
	FlushEvents(bulkSize int64) error
}
