package strategy

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/storage/filter"
	"github.com/splitio/go-split-commons/v6/storage/inmemory/mutexqueue"
	"github.com/splitio/go-split-commons/v6/util"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestNoneMode(t *testing.T) {
	now := time.Now().UTC().UnixNano()
	filter := filter.NewBloomFilter(1000, 0.01)
	uniqueKeysStorage := mutexqueue.NewMQUniqueKeysStorage(100, make(chan string), logging.NewLogger(nil))
	tracker := NewUniqueKeysTracker(filter, uniqueKeysStorage)
	counter := NewImpressionsCounter()
	none := NewNoneImpl(counter, tracker, true)

	imp := dtos.Impression{
		BucketingKey: "someBuck",
		ChangeNumber: 123,
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         now,
		Treatment:    "on",
		FeatureName:  "feature-test",
	}

	toLog, toListener := none.Apply([]dtos.Impression{imp})

	if len(toLog) != 0 || len(toListener) != 1 {
		t.Error("Should not have to log")
	}

	toLog, toListener = none.Apply([]dtos.Impression{imp})

	if len(toLog) != 0 || len(toListener) != 1 {
		t.Error("Should not have to log")
	}

	counts := counter.PopAll()
	value := counts[Key{
		FeatureName: imp.FeatureName,
		TimeFrame:   util.TruncateTimeFrame(now),
	}]

	if value != 2 {
		t.Error("Should be 2")
	}
}

func TestApplySingleNone(t *testing.T) {
	now := time.Now().UTC().UnixNano()
	filter := filter.NewBloomFilter(1000, 0.01)
	uniqueKeysStorage := mutexqueue.NewMQUniqueKeysStorage(100, make(chan string), logging.NewLogger(nil))
	tracker := NewUniqueKeysTracker(filter, uniqueKeysStorage)
	counter := NewImpressionsCounter()
	none := NewNoneImpl(counter, tracker, true)

	imp := dtos.Impression{
		BucketingKey: "someBuck",
		ChangeNumber: 123,
		KeyName:      "someKey",
		Label:        "someLabel",
		Time:         now,
		Treatment:    "on",
		FeatureName:  "feature-test",
	}

	toLog := none.ApplySingle(&imp)

	if toLog {
		t.Error("Should be false")
	}

	toLog = none.ApplySingle(&imp)

	if toLog {
		t.Error("Should be false")
	}
}
