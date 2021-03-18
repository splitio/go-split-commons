package telemetry

import (
	"testing"

	"github.com/splitio/go-split-commons/v3/conf"
	"github.com/splitio/go-split-commons/v3/dtos"
	httpMocks "github.com/splitio/go-split-commons/v3/service/mocks"
	"github.com/splitio/go-split-commons/v3/storage/mocks"
	"github.com/splitio/go-toolkit/v4/logging"
)

func TestGetURLOVerrides(t *testing.T) {
	urlOVerrides := getURLOverrides(conf.AdvancedConfig{
		SdkURL:              "some",
		TelemetryServiceURL: "other",
		EventsURL:           conf.GetDefaultAdvancedConfig().EventsURL,
		StreamingServiceURL: conf.GetDefaultAdvancedConfig().StreamingServiceURL,
		AuthServiceURL:      conf.GetDefaultAdvancedConfig().AuthServiceURL,
	})

	if urlOVerrides.Auth || urlOVerrides.Events || urlOVerrides.Stream {
		t.Error("It should be false")
	}
	if !urlOVerrides.Sdk || !urlOVerrides.Telemetry {
		t.Error("It should be true")
	}
}

func TestGetRedudantActiveFactories(t *testing.T) {
	if getRedudantActiveFactories(make(map[string]int64)) != 0 {
		t.Error("It should be 0")
	}

	testFactories := make(map[string]int64)
	testFactories["one"] = 3
	testFactories["two"] = 1
	testFactories["three"] = 2
	if getRedudantActiveFactories(testFactories) != 3 {
		t.Error("It should be 3")
	}

	testFactories2 := make(map[string]int64)
	testFactories2["one"] = 1
	testFactories2["two"] = 1
	if getRedudantActiveFactories(testFactories2) != 0 {
		t.Error("It should be 0")
	}
}

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
			if initData.Storage != redis {
				t.Error("It should be redis")
			}
			if len(initData.Tags) != 1 || initData.Tags[0] != "sentinel" {
				t.Error("It should send tags")
			}
			return nil
		},
	}

	sender := NewSenderRedis(redisMock, logger)
	factories := make(map[string]int64)
	factories["one"] = 1
	sender.Record(conf.InitConfig{}, 0, factories, []string{"sentinel"})
	if called != 1 {
		t.Error("It should be called once")
	}
}

func TestRecorderInMemory(t *testing.T) {
	called := 0
	logger := logging.NewLogger(&logging.LoggerOptions{})

	facade := NewTelemetry(
		mocks.MockTelemetryStorage{
			GetBURTimeoutsCall:    func() int64 { return 3 },
			GetNonReadyUsagesCall: func() int64 { return 5 },
		},
		mocks.MockSplitStorage{}, mocks.MockSegmentStorage{})

	mockRecorder := httpMocks.MockTelemetryRecorder{
		RecordInitCall: func(init dtos.Init, metadata dtos.Metadata) error {
			called++
			if init.ActiveFactories != 2 {
				t.Error("It should be 2")
			}
			if init.OperationMode != Standalone {
				t.Error("It should be Standalone")
			}
			if init.Storage != memory {
				t.Error("It should be memory")
			}
			if len(init.Tags) != 0 {
				t.Error("It should be zero")
			}
			return nil
		},
	}

	sender := NewSenderInMemory(facade, mockRecorder, logger, dtos.Metadata{SDKVersion: "go-test", MachineIP: "1.1.1.1", MachineName: "some"})
	factories := make(map[string]int64)
	factories["one"] = 1
	factories["two"] = 1
	sender.Record(conf.InitConfig{ManagerConfig: conf.ManagerConfig{ImpressionsMode: conf.ImpressionsModeDebug}}, 0, factories, []string{})
	if called != 1 {
		t.Error("It should be called once")
	}
}
