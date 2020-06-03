package impression

const (
	testImpressionsLatencies        = "testImpressions.time"
	testImpressionsLatenciesBackend = "backend::/api/testImpressions/bulk" // @TODO add local
	testImpressionsCounters         = "testImpressions.status.{status}"
	testImpressionsLocalCounters    = "backend::request.{status}" // @TODO add local
)

// ImpressionRecorder interface
type ImpressionRecorder interface {
	SynchronizeImpressions(bulkSize int64) error
	FlushImpressions(bulkSize int64) error
}
