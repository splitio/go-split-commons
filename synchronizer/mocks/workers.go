package mocks

import (
	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/synchronizer/worker/event"
	"github.com/splitio/go-split-commons/v6/synchronizer/worker/impression"
	"github.com/splitio/go-split-commons/v6/synchronizer/worker/largesegment"
	"github.com/splitio/go-split-commons/v6/synchronizer/worker/segment"
	"github.com/splitio/go-split-commons/v6/synchronizer/worker/split"
	"github.com/stretchr/testify/mock"
)

// LargeSegmentUpdaterMock
type LargeSegmentUpdaterMock struct {
	mock.Mock
}

func (u *LargeSegmentUpdaterMock) SynchronizeLargeSegment(name string, till *int64) error {
	args := u.Called(name, till)
	return args.Error(0)
}
func (u *LargeSegmentUpdaterMock) SynchronizeLargeSegments() error {
	args := u.Called()
	return args.Error(0)
}
func (u *LargeSegmentUpdaterMock) IsCached(name string) bool {
	args := u.Called(name)
	return args.Get(0).(bool)
}

// SplitUpdaterMock
type SplitUpdaterMock struct {
	mock.Mock
}

func (u *SplitUpdaterMock) SynchronizeSplits(till *int64) (*split.UpdateResult, error) {
	args := u.Called(till)
	return args.Get(0).(*split.UpdateResult), args.Error(1)
}
func (u *SplitUpdaterMock) SynchronizeFeatureFlags(ffChange *dtos.SplitChangeUpdate) (*split.UpdateResult, error) {
	args := u.Called(ffChange)
	return args.Get(0).(*split.UpdateResult), args.Error(1)
}
func (u *SplitUpdaterMock) LocalKill(splitName string, defaultTreatment string, changeNumber int64) {
	u.Called(splitName, defaultTreatment, changeNumber)
}

// Segment Updater
type SegmentUpdaterMock struct {
	mock.Mock
}

func (u *SegmentUpdaterMock) SynchronizeSegment(name string, till *int64) (*segment.UpdateResult, error) {
	args := u.Called(name, till)
	return args.Get(0).(*segment.UpdateResult), args.Error(1)
}
func (u *SegmentUpdaterMock) SynchronizeSegments() (map[string]segment.UpdateResult, error) {
	args := u.Called()
	return args.Get(0).(map[string]segment.UpdateResult), args.Error(1)
}
func (u *SegmentUpdaterMock) SegmentNames() []interface{} {
	return u.Called().Get(0).([]interface{})
}
func (u *SegmentUpdaterMock) IsSegmentCached(segmentName string) bool {
	return u.Called().Get(0).(bool)
}

type ImpressionRecorderMock struct {
	mock.Mock
}

func (r *ImpressionRecorderMock) SynchronizeImpressions(bulkSize int64) error {
	return r.Called(bulkSize).Error(0)
}

func (r *ImpressionRecorderMock) FlushImpressions(bulkSize int64) error {
	return r.Called(bulkSize).Error(0)
}

type EventRecorderMock struct {
	mock.Mock
}

func (e *EventRecorderMock) SynchronizeEvents(bulkSize int64) error {
	return e.Called(bulkSize).Error(0)
}

func (e *EventRecorderMock) FlushEvents(bulkSize int64) error {
	return e.Called(bulkSize).Error(0)
}

// ---

var _ event.EventRecorder = (*EventRecorderMock)(nil)
var _ impression.ImpressionRecorder = (*ImpressionRecorderMock)(nil)
var _ largesegment.Updater = (*LargeSegmentUpdaterMock)(nil)
var _ split.Updater = (*SplitUpdaterMock)(nil)
var _ segment.Updater = (*SegmentUpdaterMock)(nil)
