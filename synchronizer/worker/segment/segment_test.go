package segment

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-split-commons/v5/healthcheck/application"
	hcMock "github.com/splitio/go-split-commons/v5/healthcheck/mocks"
	"github.com/splitio/go-split-commons/v5/service"
	fetcherMock "github.com/splitio/go-split-commons/v5/service/mocks"
	"github.com/splitio/go-split-commons/v5/storage/inmemory"
	"github.com/splitio/go-split-commons/v5/storage/inmemory/mutexmap"
	"github.com/splitio/go-split-commons/v5/storage/mocks"
	"github.com/splitio/go-split-commons/v5/telemetry"
	"github.com/splitio/go-split-commons/v5/util"

	"github.com/splitio/go-toolkit/v5/datastructures/set"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/splitio/go-toolkit/v5/testhelpers"
)

func TestSegmentsSynchronizerError(t *testing.T) {
	var notifyEventCalled int64
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
		FetchCall: func(name string, changeNumber int64, fetchOptions *service.FetchOptions) (*dtos.SegmentChangesDTO, error) {
			if !fetchOptions.CacheControlHeaders {
				t.Error("should have requested no cache")
			}
			if name != "segment1" && name != "segment2" {
				t.Error("Wrong name")
			}
			return nil, &dtos.HTTPError{Code: 500, Message: "some"}
		},
	}

	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&notifyEventCalled, 1)
		},
	}

	segmentSync := NewSegmentUpdater(splitMockStorage, segmentMockStorage, segmentMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)

	_, err := segmentSync.SynchronizeSegments()
	if err == nil {
		t.Error("It should return err")
	}
	if atomic.LoadInt64(&notifyEventCalled) != 2 {
		t.Error("It should be called twice")
	}
}

func TestSegmentSynchronizer(t *testing.T) {
	before := time.Now().UTC()
	addedS1 := []string{"item1", "item2", "item3", "item4"}
	addedS2 := []string{"item5", "item6", "item7", "item8"}
	var s1Requested int64
	var s2Requested int64
	var notifyEventCalled int64

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
		RecordSuccessfulSyncCall: func(resource int, tm time.Time) {
			if resource != telemetry.SegmentSync {
				t.Error("Resource should be segments")
			}
			if tm.Before(before) {
				t.Error("It should be higher than before")
			}
		},
		RecordSyncLatencyCall: func(resource int, d time.Duration) {
			if resource != telemetry.SegmentSync {
				t.Error("Resource should be segments")
			}
		},
	}

	segmentMockFetcher := fetcherMock.MockSegmentFetcher{
		FetchCall: func(name string, changeNumber int64, fetchOptions *service.FetchOptions) (*dtos.SegmentChangesDTO, error) {
			if !fetchOptions.CacheControlHeaders {
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

	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&notifyEventCalled, 1)
		},
	}

	segmentSync := NewSegmentUpdater(splitMockStorage, segmentMockStorage, segmentMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock)

	res, err := segmentSync.SynchronizeSegments()
	testhelpers.AssertStringSliceEqualsNoOrder(t, []string{"item1", "item2", "item3", "item4"}, res["segment1"].UpdatedKeys, "")
	testhelpers.AssertStringSliceEqualsNoOrder(t, []string{"item5", "item6", "item7", "item8"}, res["segment2"].UpdatedKeys, "")
	if err != nil {
		t.Error("It should not return err")
	}

	if atomic.LoadInt64(&s1Requested) != 2 {
		t.Error("Should be called twice")
	}
	if atomic.LoadInt64(&s2Requested) != 2 {
		t.Error("Should be called twice")
	}
	if atomic.LoadInt64(&notifyEventCalled) != 2 {
		t.Error("It should be called twice")
	}
}

func TestSegmentSyncUpdate(t *testing.T) {
	var s1Requested int64
	var notifyEventCalled int64

	splitStorage := mutexmap.NewMMSplitStorage(util.NewFlagSetFilter([]string{}))
	splitStorage.Update([]dtos.SplitDTO{
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
	}, nil, 123)

	segmentStorage := mutexmap.NewMMSegmentStorage()

	segmentMockFetcher := fetcherMock.MockSegmentFetcher{
		FetchCall: func(name string, changeNumber int64, fetchOptions *service.FetchOptions) (*dtos.SegmentChangesDTO, error) {
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

	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&notifyEventCalled, 1)
		},
	}

	runtimeTelemetry, _ := inmemory.NewTelemetryStorage()
	segmentSync := NewSegmentUpdater(splitStorage, segmentStorage, segmentMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), runtimeTelemetry, appMonitorMock)

	res, err := segmentSync.SynchronizeSegments()
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

	testhelpers.AssertStringSliceEqualsNoOrder(t, []string{"item1", "item2", "item3", "item4"}, res["segment1"].UpdatedKeys, "")

	res2, err := segmentSync.SynchronizeSegment("segment1", nil)
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

	if atomic.LoadInt64(&notifyEventCalled) != 2 {
		t.Error("It should be called twice")
	}
	testhelpers.AssertStringSliceEqualsNoOrder(t, []string{"item5", "item3"}, res2.UpdatedKeys, "")
}

func TestSegmentSyncProcess(t *testing.T) {
	addedS1 := []string{"item1", "item2", "item3", "item4"}
	addedS2 := []string{"item5", "item6", "item7", "item8"}
	var s1Requested int64
	var s2Requested int64
	var notifyEventCalled int64

	splitStorage := mutexmap.NewMMSplitStorage(util.NewFlagSetFilter([]string{}))
	splitStorage.Update([]dtos.SplitDTO{
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
	}, nil, 123)

	segmentStorage := mutexmap.NewMMSegmentStorage()

	segmentMockFetcher := fetcherMock.MockSegmentFetcher{
		FetchCall: func(name string, changeNumber int64, fetchOptions *service.FetchOptions) (*dtos.SegmentChangesDTO, error) {
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

	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&notifyEventCalled, 1)
		},
	}

	runtimeTelemetry, _ := inmemory.NewTelemetryStorage()
	segmentSync := NewSegmentUpdater(splitStorage, segmentStorage, segmentMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), runtimeTelemetry, appMonitorMock)

	res, err := segmentSync.SynchronizeSegments()
	testhelpers.AssertStringSliceEqualsNoOrder(t, []string{"item1", "item2", "item3", "item4"}, res["segment1"].UpdatedKeys, "")
	testhelpers.AssertStringSliceEqualsNoOrder(t, []string{"item5", "item6", "item7", "item8"}, res["segment2"].UpdatedKeys, "")
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
	if atomic.LoadInt64(&notifyEventCalled) != 2 {
		t.Error("It should be called twice")
	}
}

func TestSegmentTill(t *testing.T) {
	addedS1 := []string{"item1", "item2", "item3", "item4"}
	var call int64
	var notifyEventCalled int64

	splitStorage := mutexmap.NewMMSplitStorage(util.NewFlagSetFilter([]string{}))
	splitStorage.Update([]dtos.SplitDTO{
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
	}, nil, 1)
	segmentStorage := mutexmap.NewMMSegmentStorage()

	segmentMockFetcher := fetcherMock.MockSegmentFetcher{
		FetchCall: func(name string, changeNumber int64, fetchOptions *service.FetchOptions) (*dtos.SegmentChangesDTO, error) {
			atomic.AddInt64(&call, 1)
			return &dtos.SegmentChangesDTO{Name: name, Added: addedS1, Removed: []string{}, Since: 2, Till: 2}, nil
		},
	}

	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&notifyEventCalled, 1)
		},
	}

	runtimeTelemetry, _ := inmemory.NewTelemetryStorage()
	segmentSync := NewSegmentUpdater(splitStorage, segmentStorage, segmentMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), runtimeTelemetry, appMonitorMock)

	var till int64 = 1
	res, err := segmentSync.SynchronizeSegment("segment1", &till)
	testhelpers.AssertStringSliceEqualsNoOrder(t, []string{"item1", "item2", "item3", "item4"}, res.UpdatedKeys, "")
	if err != nil {
		t.Error("It should not return err")
	}
	res, err = segmentSync.SynchronizeSegment("segment1", &till)
	testhelpers.AssertStringSliceEqualsNoOrder(t, []string{}, res.UpdatedKeys, "")
	if err != nil {
		t.Error("It should not return err")
	}
	if atomic.LoadInt64(&call) != 1 {
		t.Error("It should be called once")
	}
	if atomic.LoadInt64(&notifyEventCalled) != 2 {
		t.Error("It should be called twice")
	}
}

func TestSegmentCDNBypass(t *testing.T) {
	addedS1 := []string{"item1", "item2", "item3", "item4"}
	var call int64
	var notifyEventCalled int64

	splitStorage := mutexmap.NewMMSplitStorage(util.NewFlagSetFilter([]string{}))
	splitStorage.Update([]dtos.SplitDTO{
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
	}, nil, 1)
	segmentStorage := mutexmap.NewMMSegmentStorage()

	segmentMockFetcher := fetcherMock.MockSegmentFetcher{
		FetchCall: func(name string, changeNumber int64, fetchOptions *service.FetchOptions) (*dtos.SegmentChangesDTO, error) {
			atomic.AddInt64(&call, 1)
			switch called := atomic.LoadInt64(&call); {
			case called == 1:
				if fetchOptions.ChangeNumber != nil {
					t.Error("It should be nil")
				}
				return &dtos.SegmentChangesDTO{Name: name, Added: addedS1, Removed: []string{}, Since: 1, Till: 2}, nil
			case called >= 2 && called <= 11:
				if fetchOptions.ChangeNumber != nil {
					t.Error("It should be nil")
				}
				return &dtos.SegmentChangesDTO{Name: name, Added: addedS1, Removed: []string{}, Since: 2, Till: 2}, nil
			case called == 12:
				if fetchOptions.ChangeNumber == nil || *fetchOptions.ChangeNumber != 2 {
					t.Error("ChangeNumber flag should be set with value 2")
				}
				return &dtos.SegmentChangesDTO{Name: name, Added: addedS1, Removed: []string{}, Since: 3, Till: 3}, nil
			}
			return &dtos.SegmentChangesDTO{Name: name, Added: addedS1, Removed: []string{}, Since: 1, Till: 2}, nil
		},
	}

	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&notifyEventCalled, 1)
		},
	}

	runtimeTelemetry, _ := inmemory.NewTelemetryStorage()
	segmentSync := NewSegmentUpdater(splitStorage, segmentStorage, segmentMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), runtimeTelemetry, appMonitorMock)

	segmentSync.onDemandFetchBackoffBase = 1
	segmentSync.onDemandFetchBackoffMaxWait = 10 * time.Nanosecond

	var till int64 = 3
	res, err := segmentSync.SynchronizeSegment("segment1", &till)
	testhelpers.AssertStringSliceEqualsNoOrder(t, []string{"item1", "item2", "item3", "item4"}, res.UpdatedKeys, "")
	if err != nil {
		t.Error("It should not return err")
	}
	if atomic.LoadInt64(&call) != 12 {
		t.Error("It should be twelve times")
	}
	if atomic.LoadInt64(&notifyEventCalled) != 1 {
		t.Error("It should be called once")
	}
}

func TestSegmentCDNBypassLimit(t *testing.T) {
	addedS1 := []string{"item1", "item2", "item3", "item4"}
	var call int64
	var notifyEventCalled int64

	splitStorage := mutexmap.NewMMSplitStorage(util.NewFlagSetFilter([]string{}))
	splitStorage.Update([]dtos.SplitDTO{
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
	}, nil, 1)
	segmentStorage := mutexmap.NewMMSegmentStorage()

	segmentMockFetcher := fetcherMock.MockSegmentFetcher{
		FetchCall: func(name string, changeNumber int64, fetchOptions *service.FetchOptions) (*dtos.SegmentChangesDTO, error) {
			atomic.AddInt64(&call, 1)
			switch called := atomic.LoadInt64(&call); {
			case called == 1:
				if fetchOptions.ChangeNumber != nil {
					t.Error("It should be nil")
				}
				return &dtos.SegmentChangesDTO{Name: name, Added: addedS1, Removed: []string{}, Since: 1, Till: 2}, nil
			case called > 1 && called <= 11:
				if fetchOptions.ChangeNumber != nil {
					t.Error("It should be nil")
				}
				return &dtos.SegmentChangesDTO{Name: name, Added: addedS1, Removed: []string{}, Since: 2, Till: 2}, nil
			case called >= 12:
				if fetchOptions.ChangeNumber == nil || *fetchOptions.ChangeNumber != 2 {
					t.Error("ChangeNumber flag should be set with value 2")
				}
				return &dtos.SegmentChangesDTO{Name: name, Added: addedS1, Removed: []string{}, Since: 2, Till: 2}, nil
			}
			return &dtos.SegmentChangesDTO{Name: name, Added: addedS1, Removed: []string{}, Since: 2, Till: 2}, nil
		},
	}

	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&notifyEventCalled, 1)
		},
	}

	runtimeTelemetry, _ := inmemory.NewTelemetryStorage()
	segmentSync := NewSegmentUpdater(splitStorage, segmentStorage, segmentMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), runtimeTelemetry, appMonitorMock)

	segmentSync.onDemandFetchBackoffBase = 1
	segmentSync.onDemandFetchBackoffMaxWait = 10 * time.Nanosecond

	var till int64 = 3
	res, err := segmentSync.SynchronizeSegment("segment1", &till)
	testhelpers.AssertStringSliceEqualsNoOrder(t, []string{"item1", "item2", "item3", "item4"}, res.UpdatedKeys, "")
	if err != nil {
		t.Error("It should not return err")
	}
	if atomic.LoadInt64(&call) != 21 {
		t.Error("It should be twenty one times")
	}
	if atomic.LoadInt64(&notifyEventCalled) != 1 {
		t.Error("It should be called once")
	}
}

func TestSegmentSyncConcurrencyLimit(t *testing.T) {

	splitStorage := &mocks.MockSplitStorage{
		SegmentNamesCall: func() *set.ThreadUnsafeSet {
			ss := set.NewSet()
			for idx := 0; idx < 100; idx++ {
				ss.Add(fmt.Sprintf("s%d", idx))
			}
			return ss
		},
	}

	// this will fail if at any time there are more than `maxConcurrency` fetches running
	emptyVal := struct{}{}
	var done sync.Map
	var inProgress int32
	segmentMockFetcher := fetcherMock.MockSegmentFetcher{
		FetchCall: func(name string, changeNumber int64, fetchOptions *service.FetchOptions) (*dtos.SegmentChangesDTO, error) {
			if current := atomic.AddInt32(&inProgress, 1); current > maxConcurrency {
				t.Errorf("throguhput exceeded max expected concurrency of %d. Is: %d", maxConcurrency, current)
			}

			// hold the semaphore for a while
			time.Sleep(100 * time.Millisecond)
			done.Store(name, emptyVal)
			atomic.AddInt32(&inProgress, -1)
			return &dtos.SegmentChangesDTO{}, nil
		},
	}

	segmentStorage := mutexmap.NewMMSegmentStorage()
	runtimeTelemetry, _ := inmemory.NewTelemetryStorage()
	segmentSync := NewSegmentUpdater(splitStorage, segmentStorage, segmentMockFetcher, logging.NewLogger(nil), runtimeTelemetry, &application.Dummy{})
	_, err := segmentSync.SynchronizeSegments()
	if err != nil {
		t.Error("It should not return err")
	}

	// assert that all segments have been "fetched"
	for idx := 0; idx < 100; idx++ {
		key := fmt.Sprintf("s%d", idx)
		if _, ok := done.Load(key); !ok {
			t.Errorf("segment '%s' not fetched", key)
		}
	}
}
