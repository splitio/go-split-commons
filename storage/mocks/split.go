package mocks

import (
	"github.com/splitio/go-split-commons/v8/dtos"
	"github.com/splitio/go-split-commons/v8/storage"
	"github.com/splitio/go-toolkit/v5/datastructures/set"
	"github.com/stretchr/testify/mock"
)

// MockSplitStorage is a mocked implementation of Split Storage
type MockSplitStorage struct {
	AllCall                func() []dtos.SplitDTO
	ChangeNumberCall       func() (int64, error)
	FetchManyCall          func(splitNames []string) map[string]*dtos.SplitDTO
	KillLocallyCall        func(splitName string, defaultTreatment string, changeNumber int64)
	UpdateCall             func(toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO, changeNumber int64)
	RemoveCall             func(splitName string)
	SegmentNamesCall       func() *set.ThreadUnsafeSet
	LargeSegmentNamesCall  func() *set.ThreadUnsafeSet
	SetChangeNumberCall    func(changeNumber int64) error
	SplitCall              func(splitName string) *dtos.SplitDTO
	SplitNamesCall         func() []string
	TrafficTypeExistsCall  func(trafficType string) bool
	GetNamesByFlagSetsCall func(sets []string) map[string][]string
	GetAllFlagSetNamesCall func() []string
	ReplaceAllCall         func(toAdd []dtos.SplitDTO, changeNumber int64)
}

// All mock
func (m MockSplitStorage) All() []dtos.SplitDTO {
	return m.AllCall()
}

// ChangeNumber mock
func (m MockSplitStorage) ChangeNumber() (int64, error) {
	return m.ChangeNumberCall()
}

// FetchMany mock
func (m MockSplitStorage) FetchMany(splitNames []string) map[string]*dtos.SplitDTO {
	return m.FetchManyCall(splitNames)
}

// KillLocally mock
func (m MockSplitStorage) KillLocally(splitName string, defaultTreatment string, changeNumber int64) {
	m.KillLocallyCall(splitName, defaultTreatment, changeNumber)
}

// Update mock
func (m MockSplitStorage) Update(toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO, changeNumber int64) {
	m.UpdateCall(toAdd, toRemove, changeNumber)
}

// Remove mock
func (m MockSplitStorage) Remove(splitname string) {
	m.RemoveCall(splitname)
}

// SegmentNames mock
func (m MockSplitStorage) SegmentNames() *set.ThreadUnsafeSet {
	return m.SegmentNamesCall()
}

// SegmentNames mock
func (m MockSplitStorage) LargeSegmentNames() *set.ThreadUnsafeSet {
	return m.LargeSegmentNamesCall()
}

// SetChangeNumber mock
func (m MockSplitStorage) SetChangeNumber(changeNumber int64) error {
	return m.SetChangeNumberCall(changeNumber)
}

// Split mock
func (m MockSplitStorage) Split(splitName string) *dtos.SplitDTO {
	return m.SplitCall(splitName)
}

// SplitNames mock
func (m MockSplitStorage) SplitNames() []string {
	return m.SplitNamesCall()
}

// TrafficTypeExists mock
func (m MockSplitStorage) TrafficTypeExists(trafficType string) bool {
	return m.TrafficTypeExistsCall(trafficType)
}

// GetNamesByFLagSets mock
func (m MockSplitStorage) GetNamesByFlagSets(sets []string) map[string][]string {
	return m.GetNamesByFlagSetsCall(sets)
}

func (m MockSplitStorage) GetAllFlagSetNames() []string {
	return m.GetAllFlagSetNamesCall()
}

func (m MockSplitStorage) ReplaceAll(toAdd []dtos.SplitDTO, changeNumber int64) {
	m.ReplaceAllCall(toAdd, changeNumber)
}

// SplitStorageMock is a mocked implementation of Split Storage with testify
type SplitStorageMock struct {
	mock.Mock
}

// All mock
func (m *SplitStorageMock) All() []dtos.SplitDTO {
	args := m.Called()
	return args.Get(0).([]dtos.SplitDTO)
}

// ChangeNumber mock
func (m *SplitStorageMock) ChangeNumber() (int64, error) {
	args := m.Called()
	return args.Get(0).(int64), args.Error(1)
}

// FetchMany mock
func (m *SplitStorageMock) FetchMany(splitNames []string) map[string]*dtos.SplitDTO {
	args := m.Called(splitNames)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(map[string]*dtos.SplitDTO)
}

// KillLocally mock
func (m *SplitStorageMock) KillLocally(splitName string, defaultTreatment string, changeNumber int64) {
	m.Called(splitName, defaultTreatment, changeNumber)
}

// Update mock
func (m *SplitStorageMock) Update(toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO, changeNumber int64) {
	m.Called(toAdd, toRemove, changeNumber)
}

// Remove mock
func (m *SplitStorageMock) Remove(splitname string) {
	m.Called(splitname)
}

// SegmentNames mock
func (m *SplitStorageMock) SegmentNames() *set.ThreadUnsafeSet {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(*set.ThreadUnsafeSet)
}

// LargeSegmentNames mock
func (m *SplitStorageMock) LargeSegmentNames() *set.ThreadUnsafeSet {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(*set.ThreadUnsafeSet)
}

// SetChangeNumber mock
func (m *SplitStorageMock) SetChangeNumber(changeNumber int64) error {
	args := m.Called(changeNumber)
	return args.Error(0)
}

// Split mock
func (m *SplitStorageMock) Split(splitName string) *dtos.SplitDTO {
	args := m.Called(splitName)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(*dtos.SplitDTO)
}

// SplitNames mock
func (m *SplitStorageMock) SplitNames() []string {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).([]string)
}

// TrafficTypeExists mock
func (m *SplitStorageMock) TrafficTypeExists(trafficType string) bool {
	args := m.Called(trafficType)
	return args.Bool(0)
}

// GetNamesByFLagSets mock
func (m *SplitStorageMock) GetNamesByFlagSets(sets []string) map[string][]string {
	args := m.Called(sets)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(map[string][]string)
}

// GetAllFlagSetNames mock
func (m *SplitStorageMock) GetAllFlagSetNames() []string {
	args := m.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).([]string)
}

// ReplaceAll mock
func (m *SplitStorageMock) ReplaceAll(toAdd []dtos.SplitDTO, changeNumber int64) {
	m.Called(toAdd, changeNumber)
}

var _ storage.SplitStorage = (*SplitStorageMock)(nil)
