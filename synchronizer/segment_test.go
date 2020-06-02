package synchronizer

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/splitio/go-split-commons/dtos"
	fetcherMock "github.com/splitio/go-split-commons/service/mocks"
	storageMock "github.com/splitio/go-split-commons/storage/mocks"
	"github.com/splitio/go-split-commons/storage/mutexmap"
	"github.com/splitio/go-toolkit/datastructures/set"
	"github.com/splitio/go-toolkit/logging"
)

func TestSegmentsSynchronizerError(t *testing.T) {
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
	}

	segmentMockFetcher := fetcherMock.MockSegmentFetcher{
		FetchCall: func(name string, changeNumber int64) (*dtos.SegmentChangesDTO, error) {
			if name != "segment1" && name != "segment2" {
				t.Error("Wrong name")
			}
			return nil, errors.New("Some")
		},
	}

	segmentSync := NewSegmentSynchronizer(
		splitMockStorage,
		segmentMockStorage,
		segmentMockFetcher,
		storageMock.MockMetricStorage{
			IncCounterCall: func(key string) {
				if key != "splitChangeFetcher.status.200" && key != "backend::request.ok" {
					t.Error("Unexpected counter key to increase")
				}
			},
			IncLatencyCall: func(metricName string, index int) {
				if metricName != "splitChangeFetcher.time" && metricName != "backend::/api/splitChanges" {
					t.Error("Unexpected latency key to track")
				}
			},
		},
		logging.NewLogger(&logging.LoggerOptions{}),
	)

	err := segmentSync.SynchronizeSegments()
	if err == nil {
		t.Error("It should return err")
	}
}

func TestSegmentSynchronizer(t *testing.T) {
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
			switch segmentName {
			case "segment1":
				if s1Requested >= 1 {
					return 123, nil
				}
			case "segment2":
				if s2Requested >= 1 {
					return 123, nil
				}
			default:
				t.Error("Wrong case")
			}
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
		FetchCall: func(name string, changeNumber int64) (*dtos.SegmentChangesDTO, error) {
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

	segmentSync := NewSegmentSynchronizer(
		splitMockStorage,
		segmentMockStorage,
		segmentMockFetcher,
		storageMock.MockMetricStorage{
			IncCounterCall: func(key string) {
				if key != "segmentChangeFetcher.status.200" && key != "backend::request.ok" {
					t.Error("Unexpected counter key to increase")
				}
			},
			IncLatencyCall: func(metricName string, index int) {
				if metricName != "segmentChangeFetcher.time" && metricName != "backend::/api/segmentChanges" {
					t.Error("Unexpected latency key to track")
				}
			},
		},
		logging.NewLogger(&logging.LoggerOptions{}),
	)

	err := segmentSync.SynchronizeSegments()
	if err != nil {
		t.Error("It should not return err")
	}

	if s1Requested != 2 {
		t.Error("Should be called twice")
	}
	if s2Requested != 2 {
		t.Error("Should be called twice")
	}
}

func TestSegmentSyncUpdate(t *testing.T) {
	var s1Requested int64

	splitStorage := mutexmap.NewMMSplitStorage()
	splitStorage.PutMany([]dtos.SplitDTO{
		{
			Name: "split1",
			Conditions: []dtos.ConditionDTO{
				{
					ConditionType: "WHITELIST",
					Label:         "Cond1",
					MatcherGroup: dtos.MatcherGroupDTO{
						Combiner: "AND",
						Matchers: []dtos.MatcherDTO{
							{
								UserDefinedSegment: &dtos.UserDefinedSegmentMatcherDataDTO{
									SegmentName: "segment1",
								},
							},
						},
					},
				},
			},
		},
	}, 123)

	segmentStorage := mutexmap.NewMMSegmentStorage()

	segmentMockFetcher := fetcherMock.MockSegmentFetcher{
		FetchCall: func(name string, changeNumber int64) (*dtos.SegmentChangesDTO, error) {
			if name != "segment1" {
				t.Error("Wrong name")
			}
			atomic.AddInt64(&s1Requested, 1)
			switch s1Requested {
			case 1:
				return &dtos.SegmentChangesDTO{
					Name:    name,
					Added:   []string{"item1", "item2", "item3", "item4"},
					Removed: []string{},
					Since:   123,
					Till:    123,
				}, nil
			case 2:
				return &dtos.SegmentChangesDTO{
					Name:    name,
					Added:   []string{"item5"},
					Removed: []string{"item3"},
					Since:   124,
					Till:    124,
				}, nil
			default:
				t.Error("Wrong case")
			}
			return &dtos.SegmentChangesDTO{}, nil
		},
	}

	segmentSync := NewSegmentSynchronizer(
		splitStorage,
		segmentStorage,
		segmentMockFetcher,
		storageMock.MockMetricStorage{
			IncCounterCall: func(key string) {
				if key != "segmentChangeFetcher.status.200" && key != "backend::request.ok" {
					t.Error("Unexpected counter key to increase")
				}
			},
			IncLatencyCall: func(metricName string, index int) {
				if metricName != "segmentChangeFetcher.time" && metricName != "backend::/api/segmentChanges" {
					t.Error("Unexpected latency key to track")
				}
			},
		},
		logging.NewLogger(&logging.LoggerOptions{}),
	)

	err := segmentSync.SynchronizeSegments()
	if err != nil {
		t.Error("It should not return err")
	}

	s1 := segmentStorage.Keys("segment1")
	if s1 == nil || !s1.Has("item1") {
		t.Error("Segment S1 stored/retrieved incorrectly")
	}

	if s1Requested != 1 {
		t.Error("Should be called once")
	}

	err = segmentSync.SynchronizeSegment("segment1", nil)
	if err != nil {
		t.Error("It should not return err")
	}

	expectedValues := set.NewSet("item1", "item2", "item4", "item5")
	if !segmentStorage.Keys("segment1").IsEqual(expectedValues) {
		t.Error("Unexpected segment keys")
	}

	if s1Requested != 2 {
		t.Error("Should be called twice")
	}
}

func TestSegmentSyncProcess(t *testing.T) {
	addedS1 := []string{"item1", "item2", "item3", "item4"}
	addedS2 := []string{"item5", "item6", "item7", "item8"}
	var s1Requested int64
	var s2Requested int64

	splitStorage := mutexmap.NewMMSplitStorage()
	splitStorage.PutMany([]dtos.SplitDTO{
		{
			Name: "split1",
			Conditions: []dtos.ConditionDTO{
				{
					ConditionType: "WHITELIST",
					Label:         "Cond1",
					MatcherGroup: dtos.MatcherGroupDTO{
						Combiner: "AND",
						Matchers: []dtos.MatcherDTO{
							{
								UserDefinedSegment: &dtos.UserDefinedSegmentMatcherDataDTO{
									SegmentName: "segment1",
								},
							},
						},
					},
				},
			},
		},
		{
			Name: "split2",
			Conditions: []dtos.ConditionDTO{
				{
					ConditionType: "WHITELIST",
					Label:         "Cond1",
					MatcherGroup: dtos.MatcherGroupDTO{
						Combiner: "AND",
						Matchers: []dtos.MatcherDTO{
							{
								UserDefinedSegment: &dtos.UserDefinedSegmentMatcherDataDTO{
									SegmentName: "segment2",
								},
							},
						},
					},
				},
			},
		},
	}, 123)

	segmentStorage := mutexmap.NewMMSegmentStorage()

	segmentMockFetcher := fetcherMock.MockSegmentFetcher{
		FetchCall: func(name string, changeNumber int64) (*dtos.SegmentChangesDTO, error) {
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

	segmentSync := NewSegmentSynchronizer(
		splitStorage,
		segmentStorage,
		segmentMockFetcher,
		storageMock.MockMetricStorage{
			IncCounterCall: func(key string) {
				if key != "segmentChangeFetcher.status.200" && key != "backend::request.ok" {
					t.Error("Unexpected counter key to increase")
				}
			},
			IncLatencyCall: func(metricName string, index int) {
				if metricName != "segmentChangeFetcher.time" && metricName != "backend::/api/segmentChanges" {
					t.Error("Unexpected latency key to track")
				}
			},
		},
		logging.NewLogger(&logging.LoggerOptions{}),
	)

	err := segmentSync.SynchronizeSegments()
	if err != nil {
		t.Error("It should not return err")
	}

	s1 := segmentStorage.Keys("segment1")
	if s1 == nil || !s1.Has("item1") {
		t.Error("Segment S1 stored/retrieved incorrectly")
	}

	s2 := segmentStorage.Keys("segment2")
	if s2 == nil || !s2.Has("item5") {
		t.Error("Segment S2 stored/retrieved incorrectly")
	}

	if s1Requested != 1 {
		t.Error("Should be called once")
	}
	if s2Requested != 1 {
		t.Error("Should be called once")
	}
}
