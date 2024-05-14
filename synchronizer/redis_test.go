package synchronizer

import (
	"testing"

	"github.com/splitio/go-split-commons/v6/synchronizer/mocks"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestSynchronizerManagerRedis(t *testing.T) {
	var call int64
	syncMock := &mocks.MockSynchronizer{
		StartPeriodicDataRecordingCall: func() {
			call++
		},
		StopPeriodicDataRecordingCall: func() {
			call--
		},
	}

	syncManager := NewSynchronizerManagerRedis(syncMock, logging.NewLogger(nil))

	syncManager.Start()
	syncManager.Start()
	if call != 1 {
		t.Error("Start should be called once.")
	}

	if !syncManager.IsRunning() {
		t.Error("SyncManager should be running")
	}

	syncManager.Stop()
	syncManager.Stop()
	if call != 0 {
		t.Error("Stop should be called once.")
	}

	if syncManager.IsRunning() {
		t.Error("SyncManager should not be running")
	}
}
