package tasks

import (
	"testing"
	"time"

	st "github.com/splitio/go-split-commons/v8/storage/mocks"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestCleanFilterTask(t *testing.T) {
	var call int32
	logger := logging.NewLogger(&logging.LoggerOptions{})
	filter := st.MockFilter{
		ClearCall: func() {
			call++
		},
	}

	task := NewCleanFilterTask(filter, logger, 100)

	task.Start()
	time.Sleep(3 * time.Second)

	if !task.IsRunning() {
		t.Error("Telemetry task should be running")
	}

	task.Stop(true)
	if call != 1 {
		t.Error("Request not received")
	}

	if task.IsRunning() {
		t.Error("Task should be stopped")
	}
}
