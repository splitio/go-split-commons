package strategy

import (
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v4/dtos"
	"github.com/splitio/go-split-commons/v4/storage/filter"
	"github.com/splitio/go-split-commons/v4/util"
)

func TestNoneMode(t *testing.T) {
	now := time.Now().UTC().UnixNano()
	filter := filter.NewBloomFilter(1000, 0.01)
	tracker := NewUniqueKeysTracker(filter)
	counter := NewImpressionsCounter()
	none := NewNoneImpl(counter, tracker)

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
