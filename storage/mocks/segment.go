package mocks

import "github.com/splitio/go-toolkit/datastructures/set"

// MockSegmentStorage is a mocked implementation of Segment Storage
type MockSegmentStorage struct {
	ChangeNumberCall    func(segmentName string) (int64, error)
	ClearCall           func()
	KeysCall            func(segmentName string) *set.ThreadUnsafeSet
	UpdateCall          func(name string, toAdd *set.ThreadUnsafeSet, toRemove *set.ThreadUnsafeSet, changeNumber int64) error
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

// Keys mock
func (m MockSegmentStorage) Keys(segmentName string) *set.ThreadUnsafeSet {
	return m.KeysCall(segmentName)
}

// Update mock
func (m MockSegmentStorage) Update(name string, toAdd *set.ThreadUnsafeSet, toRemove *set.ThreadUnsafeSet, changeNumber int64) error {
	return m.UpdateCall(name, toAdd, toRemove, changeNumber)
}

// SetChangeNumber mock
func (m MockSegmentStorage) SetChangeNumber(segmentName string, till int64) error {
	return m.SetChangeNumberCall(segmentName, till)
}
