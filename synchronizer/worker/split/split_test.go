package split

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/splitio/go-split-commons/v2/dtos"
	fetcherMock "github.com/splitio/go-split-commons/v2/service/mocks"
	"github.com/splitio/go-split-commons/v2/storage"
	storageMock "github.com/splitio/go-split-commons/v2/storage/mocks"
	"github.com/splitio/go-split-commons/v2/storage/mutexmap"
	"github.com/splitio/go-toolkit/v4/logging"
)

func TestSplitSynchronizerError(t *testing.T) {
	splitMockStorage := storageMock.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return -1, nil
		},
	}

	splitMockFetcher := fetcherMock.MockSplitFetcher{
		FetchCall: func(changeNumber int64) (*dtos.SplitChangesDTO, error) {
			if changeNumber != -1 {
				t.Error("Wrong changenumber passed")
			}
			return nil, errors.New("Some")
		},
	}

	metricTestWrapper := storage.NewMetricWrapper(&mutexmap.MMMetricsStorage{}, nil, nil)
	splitSync := NewSplitFetcher(
		splitMockStorage,
		splitMockFetcher,
		metricTestWrapper,
		logging.NewLogger(&logging.LoggerOptions{}),
	)

	err := splitSync.SynchronizeSplits(nil)
	if err == nil {
		t.Error("It should return err")
	}
}

func TestSplitSynchronizer(t *testing.T) {
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
	}, nil, nil)
	splitSync := NewSplitFetcher(
		splitMockStorage,
		splitMockFetcher,
		metricTestWrapper,
		logging.NewLogger(&logging.LoggerOptions{}),
	)

	err := splitSync.SynchronizeSplits(nil)
	if err != nil {
		t.Error("It should not return err")
	}
}

func TestSplitSyncProcess(t *testing.T) {
	var call int64
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}
	mockedSplit2 := dtos.SplitDTO{Name: "split2", Killed: true, Status: "ACTIVE", TrafficTypeName: "two"}
	mockedSplit3 := dtos.SplitDTO{Name: "split3", Killed: true, Status: "INACTIVE", TrafficTypeName: "one"}
	mockedSplit4 := dtos.SplitDTO{Name: "split1", Killed: true, Status: "INACTIVE", TrafficTypeName: "one"}
	mockedSplit5 := dtos.SplitDTO{Name: "split4", Killed: false, Status: "ACTIVE", TrafficTypeName: "two"}

	splitMockFetcher := fetcherMock.MockSplitFetcher{
		FetchCall: func(changeNumber int64) (*dtos.SplitChangesDTO, error) {
			atomic.AddInt64(&call, 1)
			switch call {
			case 1:
				if changeNumber != -1 {
					t.Error("Wrong changenumber passed")
				}
				return &dtos.SplitChangesDTO{
					Splits: []dtos.SplitDTO{mockedSplit1, mockedSplit2, mockedSplit3},
					Since:  3,
					Till:   3,
				}, nil
			case 2:
				if changeNumber != 3 {
					t.Error("Wrong changenumber passed")
				}
				return &dtos.SplitChangesDTO{
					Splits: []dtos.SplitDTO{mockedSplit4, mockedSplit5},
					Since:  3,
					Till:   3,
				}, nil
			default:
				t.Error("Wrong calls")
				return nil, errors.New("some")
			}
		},
	}

	splitStorage := mutexmap.NewMMSplitStorage()
	splitStorage.PutMany([]dtos.SplitDTO{{}}, -1)

	metricTestWrapper := storage.NewMetricWrapper(storageMock.MockMetricStorage{
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
	}, nil, nil)
	splitSync := NewSplitFetcher(
		splitStorage,
		splitMockFetcher,
		metricTestWrapper,
		logging.NewLogger(&logging.LoggerOptions{}),
	)

	err := splitSync.SynchronizeSplits(nil)
	if err != nil {
		t.Error("It should not return err")
	}

	if !splitStorage.TrafficTypeExists("one") {
		t.Error("It should exists")
	}

	if !splitStorage.TrafficTypeExists("two") {
		t.Error("It should exists")
	}

	err = splitSync.SynchronizeSplits(nil)
	if err != nil {
		t.Error("It should not return err")
	}

	s1 := splitStorage.Split("split1")
	if s1 != nil {
		t.Error("split1 should have been removed")
	}

	s2 := splitStorage.Split("split2")
	if s2 == nil || s2.Name != "split2" || !s2.Killed {
		t.Error("split2 stored/retrieved incorrectly")
		t.Error(s2)
	}

	s3 := splitStorage.Split("split3")
	if s3 != nil {
		t.Error("split3 should have been removed")
	}

	s4 := splitStorage.Split("split4")
	if s4 == nil || s4.Name != "split4" || s4.Killed {
		t.Error("split4 stored/retrieved incorrectly")
		t.Error(s4)
	}

	if splitStorage.TrafficTypeExists("one") {
		t.Error("It should not exists")
	}

	if !splitStorage.TrafficTypeExists("two") {
		t.Error("It should exists")
	}
}

func TestSplitTill(t *testing.T) {
	var call int64
	mockedSplit1 := dtos.SplitDTO{Name: "split1", Killed: false, Status: "ACTIVE", TrafficTypeName: "one"}

	splitMockFetcher := fetcherMock.MockSplitFetcher{
		FetchCall: func(changeNumber int64) (*dtos.SplitChangesDTO, error) {
			atomic.AddInt64(&call, 1)
			return &dtos.SplitChangesDTO{
				Splits: []dtos.SplitDTO{mockedSplit1},
				Since:  2,
				Till:   2,
			}, nil
		},
	}

	splitStorage := mutexmap.NewMMSplitStorage()
	splitStorage.PutMany([]dtos.SplitDTO{{}}, -1)

	metricTestWrapper := storage.NewMetricWrapper(storageMock.MockMetricStorage{
		IncCounterCall: func(key string) {},
		IncLatencyCall: func(metricName string, index int) {},
	}, nil, nil)
	splitSync := NewSplitFetcher(
		splitStorage,
		splitMockFetcher,
		metricTestWrapper,
		logging.NewLogger(&logging.LoggerOptions{}),
	)

	var till int64
	till = int64(1)
	err := splitSync.SynchronizeSplits(&till)
	if err != nil {
		t.Error("It should not return err")
	}
	err = splitSync.SynchronizeSplits(&till)
	if err != nil {
		t.Error("It should not return err")
	}
	if atomic.LoadInt64(&call) != 1 {
		t.Error("It should be called once")
	}
}
