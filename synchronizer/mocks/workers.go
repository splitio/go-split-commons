package mocks

import (
	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/synchronizer/worker/segment"
	"github.com/splitio/go-split-commons/v6/synchronizer/worker/split"
)

// Large Segment Updater
type MockLargeSegmentUpdater struct {
	SynchronizeLargeSegmentCall  func(name string, till *int64) error
	SynchronizeLargeSegmentsCall func() error
	IsCachedCall                 func(name string) bool
}

func (u MockLargeSegmentUpdater) SynchronizeLargeSegment(name string, till *int64) error {
	return u.SynchronizeLargeSegmentCall(name, till)
}
func (u MockLargeSegmentUpdater) SynchronizeLargeSegments() error {
	return u.SynchronizeLargeSegmentsCall()
}
func (u MockLargeSegmentUpdater) IsCached(name string) bool {
	return u.IsCachedCall(name)
}

// Split Updater
type MockSplitUpdater struct {
	SynchronizeSplitsCall       func(till *int64) (*split.UpdateResult, error)
	SynchronizeFeatureFlagsCall func(ffChange *dtos.SplitChangeUpdate) (*split.UpdateResult, error)
	LocalKillCall               func(splitName string, defaultTreatment string, changeNumber int64)
}

func (u MockSplitUpdater) SynchronizeSplits(till *int64) (*split.UpdateResult, error) {
	return u.SynchronizeSplitsCall(till)
}
func (u MockSplitUpdater) SynchronizeFeatureFlags(ffChange *dtos.SplitChangeUpdate) (*split.UpdateResult, error) {
	return u.SynchronizeFeatureFlagsCall(ffChange)
}
func (u MockSplitUpdater) LocalKill(splitName string, defaultTreatment string, changeNumber int64) {
	u.LocalKillCall(splitName, defaultTreatment, changeNumber)
}

// Segment Updater
type MockSegmentUpdater struct {
	SynchronizeSegmentCall  func(name string, till *int64) (*segment.UpdateResult, error)
	SynchronizeSegmentsCall func() (map[string]segment.UpdateResult, error)
	SegmentNamesCall        func() []interface{}
	IsSegmentCachedCall     func(segmentName string) bool
}

func (u MockSegmentUpdater) SynchronizeSegment(name string, till *int64) (*segment.UpdateResult, error) {
	return u.SynchronizeSegmentCall(name, till)
}
func (u MockSegmentUpdater) SynchronizeSegments() (map[string]segment.UpdateResult, error) {
	return u.SynchronizeSegmentsCall()
}
func (u MockSegmentUpdater) SegmentNames() []interface{} {
	return u.SegmentNamesCall()
}
func (u MockSegmentUpdater) IsSegmentCached(segmentName string) bool {
	return u.IsSegmentCachedCall(segmentName)
}
