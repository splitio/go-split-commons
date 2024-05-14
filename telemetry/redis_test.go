package telemetry

import (
	"testing"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/storage/mocks"
	"github.com/splitio/go-toolkit/v5/logging"
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
			if configData.ImpressionsMode != 0 {
				t.Error("impression mode shoould be optimized")
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
