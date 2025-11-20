package strategy

import (
	"testing"

	"github.com/splitio/go-split-commons/v9/dtos"
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

	if len(toLog) != 1 || len(toListener) != 1 {
		t.Error("Should have 1 to log")
	}

	toLog, toListener = debug.Apply([]dtos.Impression{imp})

	if len(toLog) != 1 || len(toListener) != 1 {
		t.Error("Should have 1 to log")
	}
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

	if !toLog {
		t.Error("Should be true")
	}

	toLog = debug.ApplySingle(&imp)

	if !toLog {
		t.Error("Should be true")
	}
}
