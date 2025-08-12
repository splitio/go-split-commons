package mocks

import (
	"github.com/splitio/go-split-commons/v6/dtos"
)

// MockSegmentStorage is a mocked implementation of Segment Storage
type MockRuleBasedSegmentStorage struct {
	ChangeNumberCall              func() int64
	AllCall                       func() []dtos.RuleBasedSegmentDTO
	RuleBasedSegmentNamesCall     func() []string
	ContainsCall                  func(ruleBasedSegmentNames []string) bool
	GetSegmentsCall               func() []string
	CountCall                     func() int
	GetRuleBasedSegmentByNameCall func(name string) (*dtos.RuleBasedSegmentDTO, error)
	SetChangeNumberCall           func(name string, till int64)
	UpdateCall                    func(toAdd []dtos.RuleBasedSegmentDTO, toRemove []dtos.RuleBasedSegmentDTO, till int64)
	ClearCall                     func()
}

// ChangeNumber mock
func (m MockRuleBasedSegmentStorage) ChangeNumber() int64 {
	return m.ChangeNumberCall()
}

// All mock
func (m MockRuleBasedSegmentStorage) All() []dtos.RuleBasedSegmentDTO {
	return m.AllCall()
}

// RuleBasedSegmentNames mock
func (m MockRuleBasedSegmentStorage) RuleBasedSegmentNames() []string {
	return m.RuleBasedSegmentNamesCall()
}

// Contains mock
func (m MockRuleBasedSegmentStorage) Contains(ruleBasedSegmentNames []string) bool {
	return m.ContainsCall(ruleBasedSegmentNames)
}

// GetSegments mock
func (m MockRuleBasedSegmentStorage) GetSegments() []string {
	return m.GetSegmentsCall()
}

// Count mock
func (m MockRuleBasedSegmentStorage) Count() int {
	return m.CountCall()
}

// GetRuleBasedSegmentByName mock
func (m MockRuleBasedSegmentStorage) GetRuleBasedSegmentByName(name string) (*dtos.RuleBasedSegmentDTO, error) {
	return m.GetRuleBasedSegmentByNameCall(name)
}

// SetChangeNumber mock
func (m MockRuleBasedSegmentStorage) SetChangeNumber(name string, till int64) {
	m.SetChangeNumberCall(name, till)
}

// Update mock
func (m MockRuleBasedSegmentStorage) Update(toAdd []dtos.RuleBasedSegmentDTO, toRemove []dtos.RuleBasedSegmentDTO, till int64) {
	m.UpdateCall(toAdd, toRemove, till)
}

// Clear mock
func (m MockRuleBasedSegmentStorage) Clear() {
	m.ClearCall()
}
