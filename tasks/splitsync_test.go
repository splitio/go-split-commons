package tasks

import (
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/flagsets"
	hcMock "github.com/splitio/go-split-commons/v6/healthcheck/mocks"
	"github.com/splitio/go-split-commons/v6/service"
	fetcherMock "github.com/splitio/go-split-commons/v6/service/mocks"
	"github.com/splitio/go-split-commons/v6/storage/mocks"
	"github.com/splitio/go-split-commons/v6/synchronizer/worker/split"
	"github.com/splitio/go-split-commons/v6/telemetry"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestSplitSyncTask(t *testing.T) {
	var call int64
	var notifyEventCalled int64

	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}
	mockedSplit2 := dtos.SplitDTO{Name: "split2", Killed: true, Status: "ACTIVE", TrafficTypeName: "two"}
	mockedSplit3 := dtos.SplitDTO{Name: "split3", Killed: true, Status: "INACTIVE", TrafficTypeName: "one"}

	splitMockStorage := mocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) { return -1, nil },
		UpdateCall: func(toAdd []dtos.SplitDTO, toRemove []dtos.SplitDTO, changeNumber int64) {
			if changeNumber != 3 {
				t.Error("Wrong changenumber")
			}
			if len(toAdd) != 2 {
				t.Error("Wrong length of passed splits")
			}
			s1 := toAdd[0]
			if s1.Name != "split1" || s1.Killed {
				t.Error("split1 stored/retrieved incorrectly")
				t.Error(s1)
			}
			s2 := toAdd[1]
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
		FetchCall: func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
			atomic.AddInt64(&call, 1)
			req, _ := http.NewRequest("GET", "test", nil)
			fetchOptions.Apply(req)
			if req.Header.Get("Cache-Control") != "no-cache" {
				t.Error("Wrong header")
			}
			if req.URL.Query().Get("since") != "-1" {
				t.Error("Wrong since")
			}
			return &dtos.SplitChangesDTO{
				Splits: []dtos.SplitDTO{mockedSplit1, mockedSplit2, mockedSplit3},
				Since:  3,
				Till:   3,
			}, nil
		},
	}

	telemetryMockStorage := mocks.MockTelemetryStorage{
		RecordSuccessfulSyncCall: func(resource int, tm time.Time) {
			if resource != telemetry.SplitSync {
				t.Error("Resource should be splits")
			}
		},
		RecordSyncLatencyCall: func(resource int, tm time.Duration) {
			if resource != telemetry.SplitSync {
				t.Error("Resource should be splits")
			}
		},
	}

	appMonitorMock := hcMock.MockApplicationMonitor{
		NotifyEventCall: func(counterType int) {
			atomic.AddInt64(&notifyEventCalled, 1)
		},
	}

	splitTask := NewFetchSplitsTask(
		split.NewSplitUpdater(splitMockStorage, splitMockFetcher, logging.NewLogger(&logging.LoggerOptions{}), telemetryMockStorage, appMonitorMock, flagsets.NewFlagSetFilter(nil)),
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
	if atomic.LoadInt64(&notifyEventCalled) < 1 {
		t.Error("It should be called at least once")
	}
}
