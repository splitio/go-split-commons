package push

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-toolkit/v5/logging"
)

type mockSynchronizer struct {
	syncCalled int64
	syncError  atomic.Value
}

func (m *mockSynchronizer) SyncAll() error { return nil }
func (m *mockSynchronizer) SynchronizeFeatureFlags(ffChange *dtos.SplitChangeUpdate) error {
	return nil
}
func (m *mockSynchronizer) SynchronizeRuleBasedSegments(update *dtos.RuleBasedChangeUpdate) error {
	atomic.AddInt64(&m.syncCalled, 1)
	if err := m.syncError.Load(); err != nil && err.(error) != nil {
		return err.(error)
	}
	return nil
}
func (m *mockSynchronizer) LocalKill(splitName string, defaultTreatment string, changeNumber int64) {}
func (m *mockSynchronizer) SynchronizeSegment(segmentName string, till *int64) error                { return nil }
func (m *mockSynchronizer) StartPeriodicFetching()                                                  {}
func (m *mockSynchronizer) StopPeriodicFetching()                                                   {}
func (m *mockSynchronizer) StartPeriodicDataRecording()                                             {}
func (m *mockSynchronizer) StopPeriodicDataRecording()                                              {}
func (m *mockSynchronizer) SynchronizeLargeSegment(name string, till *int64) error                  { return nil }
func (m *mockSynchronizer) SynchronizeLargeSegmentUpdate(lsRFDResponseDTO *dtos.LargeSegmentRFDResponseDTO) error {
	return nil
}

func TestRuleBasedUpdateWorkerCreation(t *testing.T) {
	// Test with invalid queue size
	smallQueue := make(chan dtos.RuleBasedChangeUpdate, 100)
	_, err := NewRuleBasedUpdateWorker(smallQueue, nil, nil)
	if err == nil {
		t.Error("Should return error for small queue")
	}

	// Test with valid queue size
	validQueue := make(chan dtos.RuleBasedChangeUpdate, 5000)
	worker, err := NewRuleBasedUpdateWorker(validQueue, nil, logging.NewLogger(&logging.LoggerOptions{}))
	if err != nil {
		t.Error("Should not return error for valid queue")
	}
	if worker == nil {
		t.Error("Should return valid worker")
	}
}

func TestRuleBasedUpdateWorkerStartStop(t *testing.T) {
	queue := make(chan dtos.RuleBasedChangeUpdate, 5000)
	synchronizer := &mockSynchronizer{}
	worker, _ := NewRuleBasedUpdateWorker(queue, synchronizer, logging.NewLogger(&logging.LoggerOptions{}))

	if worker.IsRunning() {
		t.Error("Worker should not be running before Start")
	}

	worker.Start()
	time.Sleep(100 * time.Millisecond) // Wait for initialization
	if !worker.IsRunning() {
		t.Error("Worker should be running after Start")
	}

	// Try to start again
	worker.Start()
	if !worker.IsRunning() {
		t.Error("Worker should still be running after second Start")
	}

	worker.Stop()
	if worker.IsRunning() {
		t.Error("Worker should not be running after Stop")
	}

	// Try to stop again
	worker.Stop()
	if worker.IsRunning() {
		t.Error("Worker should still not be running after second Stop")
	}
}

func TestRuleBasedUpdateWorkerProcessing(t *testing.T) {
	queue := make(chan dtos.RuleBasedChangeUpdate, 5000)
	synchronizer := &mockSynchronizer{}
	synchronizer.syncError.Store(errors.New(""))
	worker, _ := NewRuleBasedUpdateWorker(queue, synchronizer, logging.NewLogger(&logging.LoggerOptions{}))

	worker.Start()

	// Test successful update
	var changeNumber int64 = 123
	queue <- *dtos.NewRuleBasedChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "some"), 123),
		&changeNumber,
		&dtos.RuleBasedSegmentDTO{Name: "test"},
	)

	time.Sleep(100 * time.Millisecond)
	if atomic.LoadInt64(&synchronizer.syncCalled) != 1 {
		t.Error("Synchronizer should be called once")
	}

	// Test update with error
	synchronizer.syncError.Store(errors.New("some error"))
	queue <- *dtos.NewRuleBasedChangeUpdate(
		dtos.NewBaseUpdate(dtos.NewBaseMessage(0, "some"), 124),
		&changeNumber,
		&dtos.RuleBasedSegmentDTO{Name: "test"},
	)

	time.Sleep(100 * time.Millisecond)
	if atomic.LoadInt64(&synchronizer.syncCalled) != 2 {
		t.Error("Synchronizer should be called twice")
	}

	worker.Stop()
}
