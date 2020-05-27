package mocks

import "github.com/splitio/go-toolkit/datastructures/set"

// MockSegmentStorage is a mocked implementation of Segment Storage
type MockSegmentStorage struct {
	ChangeNumberCall    func(segmentName string) (int64, error)
	ClearCall           func()
	GetCall             func(segmentName string) *set.ThreadUnsafeSet
	PutCall             func(name string, segment *set.ThreadUnsafeSet, changeNumber int64)
	SetChangeNumberCall func(segmentName string, till int64) error
}

// ChangeNumber mock
func (m MockSegmentStorage) ChangeNumber(segmentName string) (int64, error) {
	return m.ChangeNumberCall(segmentName)
}

// Clear mock
func (m MockSegmentStorage) Clear() {
	m.ClearCall()
}

// Get mock
func (m MockSegmentStorage) Get(segmentName string) *set.ThreadUnsafeSet {
	return m.GetCall(segmentName)
}

// Put mock
func (m MockSegmentStorage) Put(name string, segment *set.ThreadUnsafeSet, changeNumber int64) {
	m.PutCall(name, segment, changeNumber)
}

// SetChangeNumber mock
func (m MockSegmentStorage) SetChangeNumber(segmentName string, till int64) error {
	return m.SetChangeNumberCall(segmentName, till)
}
