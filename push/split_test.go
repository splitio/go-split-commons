package push

import (
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

	splitWorker.Stop()
	splitWorker.Stop()
	splitWorker.Stop()
	splitWorker.Stop()
	splitWorker.Stop()
	splitWorker.Start()
	splitWorker.Start()
	splitWorker.Start()
	splitWorker.Start()
	splitQueue <- SplitChangeUpdate{BaseUpdate: BaseUpdate{changeNumber: 223456789}}

	time.Sleep(1 * time.Second)
	if c := atomic.LoadInt32(&count); c != 2 {
		t.Error("should have been called twice. got: ", c)
	}

}

func TestAddOrUpdateFeatureFlagStroageCNGreaterThanFFCN(t *testing.T) {
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
			return 43, nil
		},
	}

	splitWorker, _ := NewSplitUpdateWorker(splitQueue, mockSync, logger, ffStorageMock)

	result := splitWorker.addOrUpdateFeatureFlag(SplitChangeUpdate{
		changeNumber: 12})
	if result != true {
		t.Error("should be true")
	}
}

func TestAddOrUpdateFeatureFlagNil(t *testing.T) {
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

	result := splitWorker.addOrUpdateFeatureFlag(SplitChangeUpdate{})
	if result != false {
		t.Error("should be true")
	}
}

func TestAddOrUpdateFeatureFlagPcnEquals(t *testing.T) {
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
			return 2, nil
		},
		UpdateCall: func(toAdd, toRemove []dtos.SplitDTO, changeNumber int64) {
			if toAdd == nil {
				t.Error("toAdd should have a feature flag")
			}
		},
	}

	splitWorker, _ := NewSplitUpdateWorker(splitQueue, mockSync, logger, ffStorageMock)

	featureFlag := dtos.SplitDTO{ChangeNumber: 4, Status: "ACTIVE"}
	result := splitWorker.addOrUpdateFeatureFlag(SplitChangeUpdate{
		featureFlag: &featureFlag, changeNumber: 4, previousChangeNumber: 2})
	if result != true {
		t.Error("should be true")
	}
}

func TestAddOrUpdateFeatureFlagArchive(t *testing.T) {
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
			return 2, nil
		},
		UpdateCall: func(toAdd, toRemove []dtos.SplitDTO, changeNumber int64) {
			if toRemove == nil {
				t.Error("toAdd should have a feature flag")
			}
		},
	}

	splitWorker, _ := NewSplitUpdateWorker(splitQueue, mockSync, logger, ffStorageMock)

	featureFlag := dtos.SplitDTO{ChangeNumber: 4, Status: "ARCHIVE"}
	result := splitWorker.addOrUpdateFeatureFlag(SplitChangeUpdate{
		featureFlag: &featureFlag, changeNumber: 4, previousChangeNumber: 2})
	if result != true {
		t.Error("should be true")
	}
}
