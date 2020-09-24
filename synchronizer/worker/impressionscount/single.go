package impressionscount

import (
	"strconv"
	"strings"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/provisional"
	"github.com/splitio/go-split-commons/service"
	"github.com/splitio/go-toolkit/logging"
)

// RecorderSingle struct for impressionsCount sync
type RecorderSingle struct {
	impressionsCounter *provisional.ImpressionsCounter
	impressionRecorder service.ImpressionsRecorder
	metadata           dtos.Metadata
	logger             logging.LoggerInterface
}

// NewRecorderSingle creates new impressionsCount synchronizer for posting impressionsCount
func NewRecorderSingle(
	impressionsCounter *provisional.ImpressionsCounter,
	impressionRecorder service.ImpressionsRecorder,
	metadata dtos.Metadata,
	logger logging.LoggerInterface,
) ImpressionsCountRecorder {
	return &RecorderSingle{
		impressionsCounter: impressionsCounter,
		impressionRecorder: impressionRecorder,
		metadata:           metadata,
		logger:             logger,
	}
}

// SynchronizeImpressionsCount syncs imp counts
func (m *RecorderSingle) SynchronizeImpressionsCount() error {
	impressionsCount := m.impressionsCounter.PopAll()

	impressionsInTimeFrame := make([]dtos.ImpressionsInTimeFrameDTO, 0)
	for key, count := range impressionsCount {
		splitted := strings.Split(key, "::")
		if len(splitted) != 2 {
			continue
		}
		featureName := splitted[0]
		asStringTimeFrame := splitted[1]
		timeFrame, ok := strconv.ParseInt(asStringTimeFrame, 10, 64)
		if ok != nil {
			continue
		}
		impressionInTimeFrame := dtos.ImpressionsInTimeFrameDTO{
			FeatureName: featureName,
			RawCount:    count,
			TimeFrame:   timeFrame,
		}
		impressionsInTimeFrame = append(impressionsInTimeFrame, impressionInTimeFrame)
	}

	pf := dtos.ImpressionsCountDTO{
		PerFeature: impressionsInTimeFrame,
	}
	return m.impressionRecorder.RecordImpressionsCount(pf, m.metadata)
}
