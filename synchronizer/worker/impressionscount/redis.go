package impressionscount

import (
	"github.com/splitio/go-split-commons/v4/dtos"
	"github.com/splitio/go-split-commons/v4/provisional/strategy"
	"github.com/splitio/go-split-commons/v4/storage"
	"github.com/splitio/go-toolkit/v5/logging"
)

// RecorderRedis struct for impressionsCount sync
type RecorderRedis struct {
	impressionsCounter      *strategy.ImpressionsCounter
	impressionsCountStorage storage.ImpressionsCountProducer
	logger                  logging.LoggerInterface
}

// NewRecorderRedis creates new impressionsCount synchronizer for log impressionsCount in redis
func NewRecorderRedis(
	impressionsCounter *strategy.ImpressionsCounter,
	impressionsCountStorage storage.ImpressionsCountProducer,
	logger logging.LoggerInterface,
) ImpressionsCountRecorder {
	return &RecorderRedis{
		impressionsCounter:      impressionsCounter,
		impressionsCountStorage: impressionsCountStorage,
		logger:                  logger,
	}
}

// SynchronizeImpressionsCount syncs imp counts
func (m *RecorderRedis) SynchronizeImpressionsCount() error {
	impressionsCount := m.impressionsCounter.PopAll()

	impressionsInTimeFrame := make([]dtos.ImpressionsInTimeFrameDTO, 0)
	for key, count := range impressionsCount {
		impressionInTimeFrame := dtos.ImpressionsInTimeFrameDTO{
			FeatureName: key.FeatureName,
			RawCount:    count,
			TimeFrame:   key.TimeFrame,
		}
		impressionsInTimeFrame = append(impressionsInTimeFrame, impressionInTimeFrame)
	}

	pf := dtos.ImpressionsCountDTO{
		PerFeature: impressionsInTimeFrame,
	}

	err := m.impressionsCountStorage.RecordImpressionsCount(pf)
	if err != nil {
		return err
	}

	return nil
}
