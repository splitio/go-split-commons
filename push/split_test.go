package push

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-toolkit/logging"
)

func TestSplitUpdateWorker(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})
	splitQueue := make(chan dtos.SplitChangeNotification, 5000)
	mockedSync := func(till *int64) error {
		if *till != 123456789 {
			t.Error("Unexpected passed till")
		}
		return nil
	}

	splitWorker, _ := NewSplitUpdateWorker(splitQueue, mockedSync, logger)
	splitWorker.Start()
	splitQueue <- dtos.NewSplitChangeNotification("some", 123456789)

	if !splitWorker.IsRunning() {
		t.Error("It should be running")
	}
	time.Sleep(1 * time.Second)
	splitWorker.Stop()

	if splitWorker.IsRunning() {
		t.Error("It should be stopped")
	}
}
