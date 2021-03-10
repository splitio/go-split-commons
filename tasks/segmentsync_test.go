package tasks

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v3/dtos"
	fetcherMock "github.com/splitio/go-split-commons/v3/service/mocks"
	storageMock "github.com/splitio/go-split-commons/v3/storage/mocks"
	"github.com/splitio/go-split-commons/v3/synchronizer/worker/segment"
	"github.com/splitio/go-toolkit/v4/datastructures/set"
	"github.com/splitio/go-toolkit/v4/logging"
)

func TestSegmentSyncTask(t *testing.T) {
	addedS1 := []string{"item1", "item2", "item3", "item4"}
	addedS2 := []string{"item5", "item6", "item7", "item8"}
	var s1Requested int64
	var s2Requested int64

	splitMockStorage := storageMock.MockSplitStorage{
		SegmentNamesCall: func() *set.ThreadUnsafeSet {
			segmentNames := set.NewSet("segment1", "segment2")
			return segmentNames
		},
	}

	segmentMockStorage := storageMock.MockSegmentStorage{
		ChangeNumberCall: func(segmentName string) (int64, error) {
			return -1, nil
		},
		KeysCall: func(segmentName string) *set.ThreadUnsafeSet {
			if segmentName != "segment1" && segmentName != "segment2" {
				t.Error("Wrong name")
			}
			switch segmentName {
			case "segment1":
			case "segment2":
				return nil
			default:
				t.Error("Wrong case")
			}
			return nil
		},
		UpdateCall: func(name string, toAdd *set.ThreadUnsafeSet, toRemove *set.ThreadUnsafeSet, changeNumber int64) error {
			switch name {
			case "segment1":
				if !toAdd.Has("item1") {
					t.Error("Wrong key in segment")
				}
				atomic.AddInt64(&s1Requested, 1)
			case "segment2":
				if !toAdd.Has("item5") {
					t.Error("Wrong key in segment")
				}
				atomic.AddInt64(&s2Requested, 1)
			default:
				t.Error("Wrong case")
			}
			return nil
		},
	}

	segmentMockFetcher := fetcherMock.MockSegmentFetcher{
		FetchCall: func(name string, changeNumber int64, noCache bool) (*dtos.SegmentChangesDTO, error) {
			if noCache {
				t.Error("no cache shold be false")
			}
			if name != "segment1" && name != "segment2" {
				t.Error("Wrong name")
			}
			switch name {
			case "segment1":
				atomic.AddInt64(&s1Requested, 1)
				return &dtos.SegmentChangesDTO{
					Name:    name,
					Added:   addedS1,
					Removed: []string{},
					Since:   123,
					Till:    123,
				}, nil
			case "segment2":
				atomic.AddInt64(&s2Requested, 1)
				return &dtos.SegmentChangesDTO{
					Name:    name,
					Added:   addedS2,
					Removed: []string{},
					Since:   123,
					Till:    123,
				}, nil
			default:
				t.Error("Wrong case")
			}
			return &dtos.SegmentChangesDTO{}, nil
		},
	}

	segmentTask := NewFetchSegmentsTask(
		segment.NewSegmentFetcher(
			splitMockStorage,
			segmentMockStorage,
			segmentMockFetcher,
			logging.NewLogger(&logging.LoggerOptions{}),
		),
		1,
		10,
		100,
		logging.NewLogger(&logging.LoggerOptions{}),
	)

	segmentTask.Start()
	time.Sleep(3 * time.Second)
	if !segmentTask.IsRunning() {
		t.Error("Segment fetching task should be running")
	}

	segmentTask.Stop(true)
	if segmentTask.IsRunning() {
		t.Error("Task should be stopped")
	}

	if atomic.LoadInt64(&s1Requested) < 2 || atomic.LoadInt64(&s2Requested) < 2 {
		t.Error("It should call fetch twice")
	}
}
