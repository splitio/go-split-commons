package strategy

import (
	"testing"

	"github.com/splitio/go-split-commons/v9/dtos"
	"github.com/stretchr/testify/assert"
)

func TestDebugMode(t *testing.T) {
	observer, _ := NewImpressionObserver(5000)
	debug := NewDebugImpl(observer, true)

	imp := dtos.Impression{
		BucketingKey: "someBuck",
		ChangeNumber: 123,
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         123456,
		Treatment:    "on",
		FeatureName:  "feature-test",
	}

	toLog, toListener := debug.Apply([]dtos.Impression{imp})

	assert.Equal(t, 1, len(toLog), "Should have 1 to log")
	assert.Equal(t, 1, len(toListener), "Should have 1 to listener")

	toLog, toListener = debug.Apply([]dtos.Impression{imp})

	assert.Equal(t, 1, len(toLog), "Should have 1 to log")
	assert.Equal(t, 1, len(toListener), "Should have 1 to listener")
}

func TestDebugModeWithProperties(t *testing.T) {
	observer, _ := NewImpressionObserver(5000)
	debug := NewDebugImpl(observer, true)

	imp := dtos.Impression{
		BucketingKey: "someBuck",
		ChangeNumber: 123,
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         123456,
		Treatment:    "on",
		FeatureName:  "feature-test",
		Properties:   "{'hello':'world'}",
	}

	toLog, toListener := debug.Apply([]dtos.Impression{imp})

	assert.Equal(t, 1, len(toLog), "Should have 1 to log")
	assert.Equal(t, 1, len(toListener), "Should have 1 to listener")

	toLog, toListener = debug.Apply([]dtos.Impression{imp})

	assert.Equal(t, 1, len(toLog), "Should have 1 to log")
	assert.Equal(t, 1, len(toListener), "Should have 1 to listener")
}

func TestApplySingleDebug(t *testing.T) {
	observer, _ := NewImpressionObserver(5000)
	debug := NewDebugImpl(observer, true)
	imp := dtos.Impression{
		BucketingKey: "someBuck",
		ChangeNumber: 123,
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         123456,
		Treatment:    "on",
		FeatureName:  "feature-test",
	}

	toLog := debug.ApplySingle(&imp)

	assert.True(t, toLog, "Should be true")

	toLog = debug.ApplySingle(&imp)

	assert.True(t, toLog, "Should be true")
}
