package mocks

type MockLargeSegmentStorage struct {
	SetChangeNumberCall      func(name string, till int64)
	UpdateCall               func(name string, userKeys []string, till int64)
	ChangeNumberCall         func(name string) int64
	CountCall                func() int
	LargeSegmentsForUserCall func(userKey string) []string
	IsInLargeSegmentCall     func(name string, key string) (bool, error)
	TotalKeysCall            func(name string) int
}

func (m MockLargeSegmentStorage) SetChangeNumber(name string, till int64) {
	m.SetChangeNumberCall(name, till)
}

func (m MockLargeSegmentStorage) Update(name string, userKeys []string, till int64) {
	m.UpdateCall(name, userKeys, till)
}

func (m MockLargeSegmentStorage) ChangeNumber(name string) int64 {
	return m.ChangeNumberCall(name)
}

func (m MockLargeSegmentStorage) Count() int {
	return m.CountCall()
}

func (m MockLargeSegmentStorage) LargeSegmentsForUser(userKey string) []string {
	return m.LargeSegmentsForUserCall(userKey)
}

func (m MockLargeSegmentStorage) IsInLargeSegment(name string, key string) (bool, error) {
	return m.IsInLargeSegmentCall(name, key)
}

func (m MockLargeSegmentStorage) TotalKeys(name string) int {
	return m.TotalKeysCall(name)
}
