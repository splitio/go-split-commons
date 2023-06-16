package push

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v4/dtos"
	"github.com/splitio/go-split-commons/v4/push/mocks"
	storageMocks "github.com/splitio/go-split-commons/v4/storage/mocks"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestSplitUpdateWorker(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitQueue := make(chan SplitChangeUpdate, 5000)

	var count int32
	mockSync := &mocks.LocalSyncMock{
		SynchronizeSplitsCall: func(till *int64) error {
			atomic.AddInt32(&count, 1)

			if *till != 123456789 && *till != 223456789 {
				t.Error("Unexpected passed till")
			}
			return nil
		},
	}
	ffStorageMock := storageMocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return -1, nil
		},
	}

	splitWorker, _ := NewSplitUpdateWorker(splitQueue, mockSync, logger, ffStorageMock)
	splitWorker.Start()
	splitQueue <- SplitChangeUpdate{BaseUpdate: BaseUpdate{changeNumber: 123456789}}

	time.Sleep(1 * time.Second)
	if !splitWorker.IsRunning() {
		t.Error("It should be running")
	}
	splitWorker.Stop()

	if splitWorker.IsRunning() {
		t.Error("It should be stopped")
	}

	splitWorker.Start()
	splitQueue <- SplitChangeUpdate{BaseUpdate: BaseUpdate{changeNumber: 223456789}}

	time.Sleep(1 * time.Second)
	if c := atomic.LoadInt32(&count); c != 2 {
		t.Error("should have been called twice. got: ", c)
	}
}

func TestSplitUpdateWorkerStorageCNGreaterThanFFCN(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitQueue := make(chan SplitChangeUpdate, 5000)

	var count int32
	mockSync := &mocks.LocalSyncMock{
		SynchronizeSplitsCall: func(till *int64) error {
			atomic.AddInt32(&count, 1)

			if *till != 123456789 && *till != 223456789 {
				t.Error("Unexpected passed till")
			}
			return nil
		},
	}
	ffStorageMock := storageMocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return 223456790, nil
		},
	}

	splitWorker, _ := NewSplitUpdateWorker(splitQueue, mockSync, logger, ffStorageMock)
	splitWorker.Start()
	splitQueue <- SplitChangeUpdate{BaseUpdate: BaseUpdate{changeNumber: 123456789}}

	time.Sleep(1 * time.Second)
	if !splitWorker.IsRunning() {
		t.Error("It should be running")
	}

	if c := atomic.LoadInt32(&count); c != 0 {
		t.Error("should haven't been called. got: ", c)
	}
}

func TestSplitUpdateWorkerStorageCNEqualsFFCN(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitQueue := make(chan SplitChangeUpdate, 5000)

	var count int32
	mockSync := &mocks.LocalSyncMock{
		SynchronizeSplitsCall: func(till *int64) error {
			atomic.AddInt32(&count, 1)

			if *till != 223456789 {
				t.Error("Unexpected passed till")
			}
			return nil
		},
	}
	ffStorageMock := storageMocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return 223456790, nil
		},
	}

	splitWorker, _ := NewSplitUpdateWorker(splitQueue, mockSync, logger, ffStorageMock)
	splitWorker.Start()
	featureFlag := dtos.SplitDTO{ChangeNumber: 223456790, Status: "ACTIVE"}
	splitQueue <- SplitChangeUpdate{BaseUpdate: BaseUpdate{changeNumber: 223456790}, featureFlag: &featureFlag, previousChangeNumber: 223456790}

	time.Sleep(1 * time.Second)
	if !splitWorker.IsRunning() {
		t.Error("It should be running")
	}

	if c := atomic.LoadInt32(&count); c != 0 {
		t.Error("should haven't been called. got: ", c)
	}
}

func TestSplitUpdateWorkerFFPcnEqualsFFNotNil(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitQueue := make(chan SplitChangeUpdate, 5000)

	var count int32
	mockSync := &mocks.LocalSyncMock{
		SynchronizeSplitsCall: func(till *int64) error {
			atomic.AddInt32(&count, 1)

			if *till != 123456789 && *till != 223456789 {
				t.Error("Unexpected passed till")
			}
			return nil
		},
	}
	ffStorageMock := storageMocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return 123456789, nil
		},
		UpdateCall: func(toAdd, toRemove []dtos.SplitDTO, changeNumber int64) {
			if len(toAdd) == 0 {
				t.Error("toAdd should have one feature flag")
			}
			if len(toRemove) != 0 {
				t.Error("toRemove should be empty")
			}
		},
	}

	splitWorker, _ := NewSplitUpdateWorker(splitQueue, mockSync, logger, ffStorageMock)
	splitWorker.Start()
	featureFlag := dtos.SplitDTO{ChangeNumber: 223456789, Status: "ACTIVE"}
	splitQueue <- SplitChangeUpdate{BaseUpdate: BaseUpdate{changeNumber: 223456789}, featureFlag: &featureFlag, previousChangeNumber: 123456789}

	time.Sleep(1 * time.Second)

	if c := atomic.LoadInt32(&count); c != 0 {
		t.Error("shouldn't haven been called. got: ", c)
	}
}

func TestSplitUpdateWorkerGetCNFromStorageError(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitQueue := make(chan SplitChangeUpdate, 5000)

	var count int32
	mockSync := &mocks.LocalSyncMock{
		SynchronizeSplitsCall: func(till *int64) error {
			atomic.AddInt32(&count, 1)

			if *till != 223456789 {
				t.Error("Unexpected passed till")
			}
			return nil
		},
	}
	ffStorageMock := storageMocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return 0, errors.New("error geting change number")
		},
	}

	splitWorker, _ := NewSplitUpdateWorker(splitQueue, mockSync, logger, ffStorageMock)
	splitWorker.Start()
	featureFlag := dtos.SplitDTO{ChangeNumber: 223456789, Status: "ACTIVE"}
	splitQueue <- SplitChangeUpdate{BaseUpdate: BaseUpdate{changeNumber: 223456789}, featureFlag: &featureFlag, previousChangeNumber: 0}

	time.Sleep(1 * time.Second)

	if c := atomic.LoadInt32(&count); c != 1 {
		t.Error("should haven been called once. got: ", c)
	}
}

func TestSplitUpdateWorkerFFIsNil(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitQueue := make(chan SplitChangeUpdate, 5000)

	var count int32
	mockSync := &mocks.LocalSyncMock{
		SynchronizeSplitsCall: func(till *int64) error {
			atomic.AddInt32(&count, 1)

			if *till != 223456789 {
				t.Error("Unexpected passed till")
			}
			return nil
		},
	}
	ffStorageMock := storageMocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return 0, nil
		},
	}

	splitWorker, _ := NewSplitUpdateWorker(splitQueue, mockSync, logger, ffStorageMock)
	splitWorker.Start()
	splitQueue <- SplitChangeUpdate{BaseUpdate: BaseUpdate{changeNumber: 223456789}, previousChangeNumber: 0}

	time.Sleep(1 * time.Second)

	if c := atomic.LoadInt32(&count); c != 1 {
		t.Error("should haven been called once. got: ", c)
	}
}

func TestSplitUpdateWorkerFFPcnDifferentStorageCN(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitQueue := make(chan SplitChangeUpdate, 5000)

	var count int32
	mockSync := &mocks.LocalSyncMock{
		SynchronizeSplitsCall: func(till *int64) error {
			atomic.AddInt32(&count, 1)

			if *till != 223456789 {
				t.Error("Unexpected passed till")
			}
			return nil
		},
	}
	ffStorageMock := storageMocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return 0, nil
		},
	}

	splitWorker, _ := NewSplitUpdateWorker(splitQueue, mockSync, logger, ffStorageMock)
	splitWorker.Start()
	featureFlag := dtos.SplitDTO{ChangeNumber: 223456789, Status: "ACTIVE"}
	splitQueue <- SplitChangeUpdate{BaseUpdate: BaseUpdate{changeNumber: 223456789}, featureFlag: &featureFlag, previousChangeNumber: 2}

	time.Sleep(1 * time.Second)

	if c := atomic.LoadInt32(&count); c != 1 {
		t.Error("should haven been called once. got: ", c)
	}
}

func TestAddOrUpdateFFStorageCNGreaterThanFFCN(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitQueue := make(chan SplitChangeUpdate, 5000)

	ffStorageMock := storageMocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return 43, nil
		},
	}

	splitWorker, _ := NewSplitUpdateWorker(splitQueue, nil, logger, ffStorageMock)

	result := splitWorker.addOrUpdateFeatureFlag(SplitChangeUpdate{BaseUpdate: BaseUpdate{changeNumber: 12}})
	if result != true {
		t.Error("should be true")
	}
}

func TestAddOrUpdateFeatureFlagNil(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitQueue := make(chan SplitChangeUpdate, 5000)

	ffStorageMock := storageMocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return -1, nil
		},
	}

	splitWorker, _ := NewSplitUpdateWorker(splitQueue, nil, logger, ffStorageMock)

	result := splitWorker.addOrUpdateFeatureFlag(SplitChangeUpdate{})
	if result != false {
		t.Error("should be false")
	}
}

func TestAddOrUpdateFeatureFlagPcnEquals(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitQueue := make(chan SplitChangeUpdate, 5000)

	ffStorageMock := storageMocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return 2, nil
		},
		UpdateCall: func(toAdd, toRemove []dtos.SplitDTO, changeNumber int64) {
			if len(toAdd) == 0 {
				t.Error("toAdd should have a feature flag")
			}
			if len(toRemove) != 0 {
				t.Error("toRemove shouldn't have a feature flag")
			}
		},
	}

	splitWorker, _ := NewSplitUpdateWorker(splitQueue, nil, logger, ffStorageMock)

	featureFlag := dtos.SplitDTO{ChangeNumber: 4, Status: "ACTIVE"}
	result := splitWorker.addOrUpdateFeatureFlag(SplitChangeUpdate{BaseUpdate: BaseUpdate{changeNumber: 4},
		featureFlag: &featureFlag, previousChangeNumber: 2})
	if result != true {
		t.Error("should be true")
	}
}

func TestAddOrUpdateFeatureFlagArchive(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitQueue := make(chan SplitChangeUpdate, 5000)

	ffStorageMock := storageMocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return 2, nil
		},
		UpdateCall: func(toAdd, toRemove []dtos.SplitDTO, changeNumber int64) {
			if len(toRemove) == 0 {
				t.Error("toRemove should have a feature flag")
			}
			if len(toAdd) != 0 {
				t.Error("toAdd shouldn't have a feature flag")
			}
		},
	}

	splitWorker, _ := NewSplitUpdateWorker(splitQueue, nil, logger, ffStorageMock)

	featureFlag := dtos.SplitDTO{ChangeNumber: 4, Status: "ARCHIVE"}
	result := splitWorker.addOrUpdateFeatureFlag(SplitChangeUpdate{BaseUpdate: BaseUpdate{changeNumber: 4},
		featureFlag: &featureFlag, previousChangeNumber: 2})
	if result != true {
		t.Error("should be true")
	}
}

func TestAddOrUpdateFFCNFromStorageError(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitQueue := make(chan SplitChangeUpdate, 5000)

	ffStorageMock := storageMocks.MockSplitStorage{
		ChangeNumberCall: func() (int64, error) {
			return 0, errors.New("error geting change number")
		},
	}

	splitWorker, _ := NewSplitUpdateWorker(splitQueue, nil, logger, ffStorageMock)

	result := splitWorker.addOrUpdateFeatureFlag(SplitChangeUpdate{BaseUpdate: BaseUpdate{changeNumber: 12}})
	if result != false {
		t.Error("should be false")
	}
}
