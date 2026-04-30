package strategy

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v9/dtos"
	"github.com/splitio/go-split-commons/v9/storage/inmemory"
	"github.com/stretchr/testify/assert"
)

func TestOptimizedMode(t *testing.T) {
	observer, _ := NewImpressionObserver(5000)
	counter := NewImpressionsCounter()
	runtimeTelemetry, _ := inmemory.NewTelemetryStorage()
	optimized := NewOptimizedImpl(observer, counter, runtimeTelemetry, true)
	imp := dtos.Impression{
		BucketingKey: "someBuck",
		ChangeNumber: 123,
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         time.Now().UTC().UnixNano(),
		Treatment:    "on",
		DefinitionName:  "feature-test",
	}

	toLog, toListener := optimized.Apply([]dtos.Impression{imp})
	assert.Equal(t, 1, len(toLog), "Should have 1 to log")
	assert.Equal(t, 1, len(toListener), "Should have 1 to listener")

	assert.Equal(t, 0, len(counter.impressionsCounts), "Should not have counts")

	toLog, toListener = optimized.Apply([]dtos.Impression{imp})

	assert.Equal(t, 0, len(toLog), "Should have 0 to log")
	assert.Equal(t, 1, len(toListener), "Should have 1 to listener")

	rawCounts := counter.PopAll()
	assert.Equal(t, 1, len(rawCounts), "Should have counts")
	for key, counts := range counter.PopAll() {
		assert.Equal(t, "feature-test", key.DefinitionName, "Feature should be feature-test")
		assert.Equal(t, 1, counts, "It should be tracked only once")
	}
}

func TestOptimizedModeWithProperties(t *testing.T) {
	observer, _ := NewImpressionObserver(5000)
	counter := NewImpressionsCounter()
	runtimeTelemetry, _ := inmemory.NewTelemetryStorage()
	optimized := NewOptimizedImpl(observer, counter, runtimeTelemetry, true)
	imp := dtos.Impression{
		BucketingKey: "someBuck",
		ChangeNumber: 123,
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         time.Now().UTC().UnixNano(),
		Treatment:    "on",
		DefinitionName:  "feature-test",
		Properties:   "{'hello':'world'}",
	}

	toLog, toListener := optimized.Apply([]dtos.Impression{imp})

	assert.Equal(t, 1, len(toLog), "Should have 1 to log")
	assert.Equal(t, 1, len(toListener), "Should have 1 to listener")

	toLog, toListener = optimized.Apply([]dtos.Impression{imp})

	assert.Equal(t, 1, len(toLog), "toLog should be 1")
	assert.Equal(t, 1, len(toListener), "toListener should be 1")

	rawCounts := counter.PopAll()
	assert.Equal(t, 0, len(rawCounts), "Should doesn't have counts")
	for key, counts := range counter.PopAll() {
		assert.Equal(t, "feature-test", key.DefinitionName, "Feature should be feature-test")
		assert.Equal(t, 1, counts, "It should be tracked empty")
	}
}

func TestApplySingleOptimized(t *testing.T) {
	observer, _ := NewImpressionObserver(5000)
	counter := NewImpressionsCounter()
	runtimeTelemetry, _ := inmemory.NewTelemetryStorage()
	optimized := NewOptimizedImpl(observer, counter, runtimeTelemetry, true)
	imp := dtos.Impression{
		BucketingKey: "someBuck",
		ChangeNumber: 123,
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         time.Now().UTC().UnixNano(),
		Treatment:    "on",
		DefinitionName:  "feature-test",
	}

	toLog := optimized.ApplySingle(&imp)

	assert.True(t, toLog, "Should be true")
	toLog = optimized.ApplySingle(&imp)

	assert.False(t, toLog, "Should be false")
}
