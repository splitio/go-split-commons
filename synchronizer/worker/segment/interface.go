package segment

// Updater interface
type Updater interface {
	SynchronizeSegment(name string, till *int64) error
	SynchronizeSegments() error
	SegmentNames() []interface{}
}
