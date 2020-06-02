package event

const (
	postEventsLatencies        = "events.time"
	postEventsLatenciesBackend = "backend::/api/events/bulk"
	postEventsCounters         = "events.status.{status}"
	postEventsLocalCounters    = "backend::request.{status}"
)

// EventSynchronizer interface
type EventSynchronizer interface {
	SynchronizeEvents(bulkSize int64) error
	FlushEvents(bulkSize int64) error
}
