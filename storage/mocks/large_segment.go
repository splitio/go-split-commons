package mocks

type MockLargeSegmentStorage struct {
	SetChangeNumberCall      func(name string, till int64)
	UpdateCall               func(name string, userKeys []string)
	ChangeNumberCall         func(name string) int64
	CountCall                func() int
	LargeSegmentsForUserCall func(userKey string) []string
}

func (m MockLargeSegmentStorage) SetChangeNumber(name string, till int64) {
	m.SetChangeNumberCall(name, till)
}

func (m MockLargeSegmentStorage) Update(name string, userKeys []string) {
	m.UpdateCall(name, userKeys)
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
