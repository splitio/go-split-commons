package tasks

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v2/dtos"
	fetcherMock "github.com/splitio/go-split-commons/v2/service/mocks"
	"github.com/splitio/go-split-commons/v2/storage"
	storageMock "github.com/splitio/go-split-commons/v2/storage/mocks"
	"github.com/splitio/go-split-commons/v2/synchronizer/worker/split"
	"github.com/splitio/go-toolkit/v3/logging"
)

func TestSplitSyncTask(t *testing.T) {
	var call int64

	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}
	mockedSplit2 := dtos.SplitDTO{Name: "split2", Killed: true, Status: "ACTIVE", TrafficTypeName: "two"}
	mockedSplit3 := dtos.SplitDTO{Name: "split3", Killed: true, Status: "INACTIVE", TrafficTypeName: "one"}

	splitMockStorage := storageMock.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return -1, nil
		},
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
		FetchCall: func(changeNumber int64) (*dtos.SplitChangesDTO, error) {
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

	metricTestWrapper := storage.NewMetricWrapper(storageMock.MockMetricStorage{
		IncCounterCall: func(key string) {},
		IncLatencyCall: func(metricName string, index int) {},
	}, nil, nil)
	splitTask := NewFetchSplitsTask(
		split.NewSplitFetcher(
			splitMockStorage,
			splitMockFetcher,
			metricTestWrapper,
			logging.NewLogger(&logging.LoggerOptions{}),
		),
		3,
		logging.NewLogger(&logging.LoggerOptions{}),
	)

	splitTask.Start()
	if !splitTask.IsRunning() {
		t.Error("Split fetching task should be running")
	}

	splitTask.Stop(false)
	time.Sleep(time.Second * 1)
	if atomic.LoadInt64(&call) <= 0 {
		t.Error("Request not received")
	}

	if splitTask.IsRunning() {
		t.Error("Task should be stopped")
	}
}
