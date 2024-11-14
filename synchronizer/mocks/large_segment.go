package mocks

type MockLargeSegmenUpdater struct {
	SynchronizeLargeSegmentCall  func(name string, till *int64) error
	SynchronizeLargeSegmentsCall func() error
	IsCachedCall                 func(name string) bool
}

func (m MockLargeSegmenUpdater) SynchronizeLargeSegment(name string, till *int64) error {
	return m.SynchronizeLargeSegmentCall(name, till)
}

func (m MockLargeSegmenUpdater) SynchronizeLargeSegments() error {
	return m.SynchronizeLargeSegmentsCall()
}

func (m MockLargeSegmenUpdater) IsCached(name string) bool {
	return m.IsCachedCall(name)
}
