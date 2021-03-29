package segment

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v3/dtos"
	fetcherMock "github.com/splitio/go-split-commons/v3/service/mocks"
	"github.com/splitio/go-split-commons/v3/storage/inmemory"
	"github.com/splitio/go-split-commons/v3/storage/inmemory/mutexmap"
	"github.com/splitio/go-split-commons/v3/storage/mocks"
	"github.com/splitio/go-split-commons/v3/telemetry"
	"github.com/splitio/go-toolkit/v4/datastructures/set"
	"github.com/splitio/go-toolkit/v4/logging"
)

func TestSegmentsSynchronizerError(t *testing.T) {
	splitMockStorage := mocks.MockSplitStorage{
		SegmentNamesCall: func() *set.ThreadUnsafeSet { return set.NewSet("segment1", "segment2") },
	}

	segmentMockStorage := mocks.MockSegmentStorage{
		ChangeNumberCall: func(segmentName string) (int64, error) { return -1, nil },
	}

	telemetryMockStorage := mocks.MockTelemetryStorage{
		RecordSyncErrorCall: func(resource, status int) {
			if resource != telemetry.SegmentSync {
				t.Error("It should be segments")
			}
			if status != 500 {
				t.Error("Status should be 500")
			}
		},
	}

	segmentMockFetcher := fetcherMock.MockSegmentFetcher{
		FetchCall: func(name string, changeNumber int64, requestNoCache bool) (*dtos.SegmentChangesDTO, error) {
			if requestNoCache {
				t.Error("should not have requested no cache")
			}
			if name != "segment1" && name != "segment2" {
				t.Error("Wrong name")
			}
			return nil, &dtos.HTTPError{Code: 500, Message: "some"}
		},
	}

	segmentSync := NewSegmentFetcher(splitMockStorage, segmentMockStorage, segmentMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage)

	err := segmentSync.SynchronizeSegments(false)
	if err == nil {
		t.Error("It should return err")
	}
}

func TestSegmentSynchronizer(t *testing.T) {
	before := time.Now().UTC().UnixNano() / int64(time.Millisecond)
	addedS1 := []string{"item1", "item2", "item3", "item4"}
	addedS2 := []string{"item5", "item6", "item7", "item8"}
	var s1Requested int64
	var s2Requested int64

	splitMockStorage := mocks.MockSplitStorage{
		SegmentNamesCall: func() *set.ThreadUnsafeSet { return set.NewSet("segment1", "segment2") },
	}

	segmentMockStorage := mocks.MockSegmentStorage{
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
			case "segment1", "segment2":
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

	telemetryMockStorage := mocks.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, tm int64) {
			if resource != telemetry.SegmentSync {
				t.Error("Resource should be segments")
			}
			if tm < before {
				t.Error("It should be higher than before")
			}
		},
		RecordSyncLatencyCall: func(resource int, tm int64) {
			if resource != telemetry.SegmentSync {
				t.Error("Resource should be segments")
			}
		},
	}

	segmentMockFetcher := fetcherMock.MockSegmentFetcher{
		FetchCall: func(name string, changeNumber int64, noCache bool) (*dtos.SegmentChangesDTO, error) {
			if !noCache {
				t.Error("should have requested no cache")
			}
			if name != "segment1" && name != "segment2" {
				t.Error("Wrong name")
			}
			switch name {
			case "segment1":
				atomic.AddInt64(&s1Requested, 1)
				return &dtos.SegmentChangesDTO{Name: name, Added: addedS1, Removed: []string{}, Since: 123, Till: 123}, nil
			case "segment2":
				atomic.AddInt64(&s2Requested, 1)
				return &dtos.SegmentChangesDTO{Name: name, Added: addedS2, Removed: []string{}, Since: 123, Till: 123}, nil
			default:
				t.Error("Wrong case")
			}
			return &dtos.SegmentChangesDTO{}, nil
		},
	}

	segmentSync := NewSegmentFetcher(splitMockStorage, segmentMockStorage, segmentMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage)

	err := segmentSync.SynchronizeSegments(true)
	if err != nil {
		t.Error("It should not return err")
	}

	if atomic.LoadInt64(&s1Requested) != 2 {
		t.Error("Should be called twice")
	}
	if atomic.LoadInt64(&s2Requested) != 2 {
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
		FetchCall: func(name string, changeNumber int64, noCache bool) (*dtos.SegmentChangesDTO, error) {
			if name != "segment1" {
				t.Error("Wrong name")
			}
			atomic.AddInt64(&s1Requested, 1)
			switch s1Requested {
			case 1:
				return &dtos.SegmentChangesDTO{Name: name, Added: []string{"item1", "item2", "item3", "item4"}, Removed: []string{}, Since: 123, Till: 123}, nil
			case 2:
				return &dtos.SegmentChangesDTO{Name: name, Added: []string{"item5"}, Removed: []string{"item3"}, Since: 124, Till: 124}, nil
			default:
				t.Error("Wrong case")
			}
			return &dtos.SegmentChangesDTO{}, nil
		},
	}

	runtimeTelemetry, _ := inmemory.NewTelemetryStorage()
	segmentSync := NewSegmentFetcher(splitStorage, segmentStorage, segmentMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), runtimeTelemetry)

	err := segmentSync.SynchronizeSegments(false)
	if err != nil {
		t.Error("It should not return err")
	}

	s1 := segmentStorage.Keys("segment1")
	if s1 == nil || !s1.Has("item1") {
		t.Error("Segment S1 stored/retrieved incorrectly")
	}

	if atomic.LoadInt64(&s1Requested) != 1 {
		t.Error("Should be called once")
	}

	err = segmentSync.SynchronizeSegment("segment1", nil, false)
	if err != nil {
		t.Error("It should not return err")
	}

	expectedValues := set.NewSet("item1", "item2", "item4", "item5")
	if !segmentStorage.Keys("segment1").IsEqual(expectedValues) {
		t.Error("Unexpected segment keys")
	}

	if atomic.LoadInt64(&s1Requested) != 2 {
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
		FetchCall: func(name string, changeNumber int64, noCache bool) (*dtos.SegmentChangesDTO, error) {
			if name != "segment1" && name != "segment2" {
				t.Error("Wrong name")
			}
			switch name {
			case "segment1":
				atomic.AddInt64(&s1Requested, 1)
				return &dtos.SegmentChangesDTO{Name: name, Added: addedS1, Removed: []string{}, Since: 123, Till: 123}, nil
			case "segment2":
				atomic.AddInt64(&s2Requested, 1)
				return &dtos.SegmentChangesDTO{Name: name, Added: addedS2, Removed: []string{}, Since: 123, Till: 123}, nil
			default:
				t.Error("Wrong case")
			}
			return &dtos.SegmentChangesDTO{}, nil
		},
	}

	runtimeTelemetry, _ := inmemory.NewTelemetryStorage()
	segmentSync := NewSegmentFetcher(splitStorage, segmentStorage, segmentMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), runtimeTelemetry)

	err := segmentSync.SynchronizeSegments(false)
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

func TestSegmentTill(t *testing.T) {
	addedS1 := []string{"item1", "item2", "item3", "item4"}
	var call int64

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
	}, 1)
	segmentStorage := mutexmap.NewMMSegmentStorage()

	segmentMockFetcher := fetcherMock.MockSegmentFetcher{
		FetchCall: func(name string, changeNumber int64, noCache bool) (*dtos.SegmentChangesDTO, error) {
			atomic.AddInt64(&call, 1)
			return &dtos.SegmentChangesDTO{Name: name, Added: addedS1, Removed: []string{}, Since: 2, Till: 2}, nil
		},
	}

	runtimeTelemetry, _ := inmemory.NewTelemetryStorage()
	segmentSync := NewSegmentFetcher(splitStorage, segmentStorage, segmentMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), runtimeTelemetry)

	var till int64 = 1
	err := segmentSync.SynchronizeSegment("segment1", &till, false)
	if err != nil {
		t.Error("It should not return err")
	}
	err = segmentSync.SynchronizeSegment("segment1", &till, false)
	if err != nil {
		t.Error("It should not return err")
	}
	if atomic.LoadInt64(&call) != 1 {
		t.Error("It should be called once")
	}
}
