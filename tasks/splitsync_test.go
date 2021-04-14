package tasks

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/dtos"
	fetcherMock "github.com/splitio/go-split-commons/service/mocks"
	"github.com/splitio/go-split-commons/storage/mocks"
	"github.com/splitio/go-split-commons/synchronizer/worker/split"
	"github.com/splitio/go-split-commons/telemetry"
	"github.com/splitio/go-toolkit/logging"
)

func TestSplitSyncTask(t *testing.T) {
	var call int64

	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}
	mockedSplit2 := dtos.SplitDTO{Name: "split2", Killed: true, Status: "ACTIVE", TrafficTypeName: "two"}
	mockedSplit3 := dtos.SplitDTO{Name: "split3", Killed: true, Status: "INACTIVE", TrafficTypeName: "one"}

	splitMockStorage := mocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) { return -1, nil },
		PutManyCall: func(splits []dtos.SplitDTO, changeNumber int64) {
			if changeNumber != 3 {
				t.Error("Wrong changenumber")
			}
			if len(splits) != 2 {
				t.Error("Wrong length of passed splits")
			}
			s1 := splits[0]
			if s1.Name != "split1" || s1.Killed {
				t.Error("split1 stored/retrieved incorrectly")
				t.Error(s1)
			}
			s2 := splits[1]
			if s2.Name != "split2" || !s2.Killed {
				t.Error("split2 stored/retrieved incorrectly")
				t.Error(s2)
			}
		},
		RemoveCall: func(splitname string) {
			if splitname != "split3" {
				t.Error("It should remove split3")
			}
		},
	}

	splitMockFetcher := fetcherMock.MockSplitFetcher{
		FetchCall: func(changeNumber int64, noCache bool) (*dtos.SplitChangesDTO, error) {
			if noCache {
				t.Error("noCache should be false.")
			}
			atomic.AddInt64(&call, 1)
			if changeNumber != -1 {
				t.Error("Wrong changenumber passed")
			}
			return &dtos.SplitChangesDTO{
				Splits: []dtos.SplitDTO{mockedSplit1, mockedSplit2, mockedSplit3},
				Since:  3,
				Till:   3,
			}, nil
		},
	}

	telemetryMockStorage := mocks.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, tm int64) {
			if resource != telemetry.SplitSync {
				t.Error("Resource should be splits")
			}
		},
		RecordSyncLatencyCall: func(resource int, tm int64) {
			if resource != telemetry.SplitSync {
				t.Error("Resource should be splits")
			}
		},
	}

	splitTask := NewFetchSplitsTask(
		split.NewSplitFetcher(splitMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage),
		1,
		logging.NewLogger(&logging.LoggerOptions{}),
	)

	splitTask.Start()
	time.Sleep(2 * time.Second)
	if !splitTask.IsRunning() {
		t.Error("Split fetching task should be running")
	}

	splitTask.Stop(false)
	if atomic.LoadInt64(&call) < 1 {
		t.Error("Request not received")
	}

	if splitTask.IsRunning() {
		t.Error("Task should be stopped")
	}
}
