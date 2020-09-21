package mocks

import "github.com/splitio/go-split-commons/v2/dtos"

// MockImpressionRecorder mocked implementation of impression recorder
type MockImpressionRecorder struct {
	RecordCall func(impressions []dtos.ImpressionsDTO, metadata dtos.Metadata) error
}

// Record mock
func (m MockImpressionRecorder) Record(impressions []dtos.ImpressionsDTO, metadata dtos.Metadata) error {
	return m.RecordCall(impressions, metadata)
}
