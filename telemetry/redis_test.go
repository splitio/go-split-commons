package telemetry

import (
	"testing"

	"github.com/splitio/go-split-commons/v3/dtos"
	"github.com/splitio/go-split-commons/v3/storage/mocks"
	"github.com/splitio/go-toolkit/v4/logging"
)

func TestRecorderRedis(t *testing.T) {
	called := 0
	logger := logging.NewLogger(&logging.LoggerOptions{})

	redisMock := mocks.MockTelemetryStorage{
		RecordInitDataCall: func(initData dtos.Init) error {
			called++
			if initData.ActiveFactories != 1 {
				t.Error("It should be 1")
			}
			if initData.OperationMode != Consumer {
				t.Error("It should be Consumer")
			}
			if initData.Storage != Redis {
				t.Error("It should be redis")
			}
			if len(initData.Tags) != 1 || initData.Tags[0] != "sentinel" {
				t.Error("It should send tags")
			}
			return nil
		},
	}

	sender := NewSynchronizerRedis(redisMock, logger)
	factories := make(map[string]int64)
	factories["one"] = 1
	sender.SynchronizeInit(InitConfig{}, 0, factories, []string{"sentinel"})
	if called != 1 {
		t.Error("It should be called once")
	}
}
