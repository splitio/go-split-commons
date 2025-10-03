package mocks

import (
	"github.com/splitio/go-split-commons/v7/dtos"
	"github.com/splitio/go-split-commons/v7/storage"
	"github.com/splitio/go-toolkit/v5/datastructures/set"
	"github.com/stretchr/testify/mock"
)

// MockSegmentStorage is a mocked implementation of Segment Storage
type MockRuleBasedSegmentStorage struct {
	mock.Mock
}

// ChangeNumber mock
func (m *MockRuleBasedSegmentStorage) ChangeNumber() int64 {
	args := m.Called()
	return args.Get(0).(int64)
}

// All mock
func (m *MockRuleBasedSegmentStorage) All() []dtos.RuleBasedSegmentDTO {
	args := m.Called()
	return args.Get(0).([]dtos.RuleBasedSegmentDTO)
}

// RuleBasedSegmentNames mock
func (m *MockRuleBasedSegmentStorage) RuleBasedSegmentNames() []string {
	args := m.Called()
	return args.Get(0).([]string)
}

// Contains mock
func (m *MockRuleBasedSegmentStorage) Contains(ruleBasedSegmentNames []string) bool {
	args := m.Called(ruleBasedSegmentNames)
	return bool(args.Bool(0))
}

// GetSegments mock
func (m *MockRuleBasedSegmentStorage) GetSegments() *set.ThreadUnsafeSet {
	args := m.Called()
	return args.Get(0).(*set.ThreadUnsafeSet)
}

// Count mock
func (m *MockRuleBasedSegmentStorage) Count() int {
	args := m.Called()
	return int(args.Int(0))
}

// GetRuleBasedSegmentByName mock
func (m *MockRuleBasedSegmentStorage) GetRuleBasedSegmentByName(name string) (*dtos.RuleBasedSegmentDTO, error) {
	args := m.Called(name)
	return args.Get(0).(*dtos.RuleBasedSegmentDTO), args.Error(1)
}

// SetChangeNumber mock
func (m *MockRuleBasedSegmentStorage) SetChangeNumber(till int64) error {
	args := m.Called(till)
	return args.Error(0)
}

// Update mock
func (m *MockRuleBasedSegmentStorage) Update(toAdd []dtos.RuleBasedSegmentDTO, toRemove []dtos.RuleBasedSegmentDTO, till int64) {
	m.Called(toAdd, toRemove, till)
}

// Clear mock
func (m *MockRuleBasedSegmentStorage) Clear() {
	m.Called()
}

var _ storage.RuleBasedSegmentsStorage = (*MockRuleBasedSegmentStorage)(nil)
