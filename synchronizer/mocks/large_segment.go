package mocks

type MockLargeSegmenUpdater struct {
	SynchronizeLargeSegmentCall  func(name string, till *int64) (int64, error)
	SynchronizeLargeSegmentsCall func() (map[string]int64, error)
	LargeSegmentNamesCall        func() []interface{}
}

func (m MockLargeSegmenUpdater) SynchronizeLargeSegment(name string, till *int64) (int64, error) {
	return m.SynchronizeLargeSegmentCall(name, till)
}

func (m MockLargeSegmenUpdater) SynchronizeLargeSegments() (map[string]int64, error) {
	return m.SynchronizeLargeSegmentsCall()
}

func (m MockLargeSegmenUpdater) LargeSegmentNames() []interface{} {
	return m.LargeSegmentNamesCall()
}
