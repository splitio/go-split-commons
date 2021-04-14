package telemetry

import (
	"testing"

	"github.com/splitio/go-split-commons/v3/conf"
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
