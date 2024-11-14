package largesegment

func NewNoOpLargeSegmentUpdater() *NoOpUpdaterImpl {
	return &NoOpUpdaterImpl{}
}

func (u *NoOpUpdaterImpl) SynchronizeLargeSegment(name string, till *int64) error { return nil }
func (u *NoOpUpdaterImpl) SynchronizeLargeSegments() error                        { return nil }
func (s *NoOpUpdaterImpl) IsCached(name string) bool                              { return false }

var _ Updater = (*NoOpUpdaterImpl)(nil)
