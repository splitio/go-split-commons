package telemetry

import (
	"testing"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/storage/mocks"
	"github.com/splitio/go-toolkit/logging"
)

func TestRecorderRedis(t *testing.T) {
	called := 0
	logger := logging.NewLogger(&logging.LoggerOptions{})

	redisMock := mocks.MockTelemetryStorage{
		RecordConfigDataCall: func(configData dtos.Config) error {
			called++
			if configData.ActiveFactories != 1 {
				t.Error("It should be 1")
			}
			if configData.OperationMode != Consumer {
				t.Error("It should be Consumer")
			}
			if configData.Storage != Redis {
				t.Error("It should be redis")
			}
			if len(configData.Tags) != 1 || configData.Tags[0] != "sentinel" {
				t.Error("It should send tags")
			}
			return nil
		},
	}

	sender := NewSynchronizerRedis(redisMock, logger)
	factories := make(map[string]int64)
	factories["one"] = 1
	sender.SynchronizeConfig(InitConfig{}, 0, factories, []string{"sentinel"})
	if called != 1 {
		t.Error("It should be called once")
	}
}
