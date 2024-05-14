package mocks

import "github.com/splitio/go-split-commons/v6/dtos"

// MockImpressionRecorder mocked implementation of impression recorder
type MockImpressionRecorder struct {
	RecordCall                 func(impressions []dtos.ImpressionsDTO, metadata dtos.Metadata, extraHeaders map[string]string) error
	RecordImpressionsCountCall func(pf dtos.ImpressionsCountDTO, metadata dtos.Metadata) error
}

// Record mock
func (m MockImpressionRecorder) Record(impressions []dtos.ImpressionsDTO, metadata dtos.Metadata, extraHeaders map[string]string) error {
	return m.RecordCall(impressions, metadata, extraHeaders)
}

// RecordImpressionsCount mock
func (m MockImpressionRecorder) RecordImpressionsCount(pf dtos.ImpressionsCountDTO, metadata dtos.Metadata) error {
	return m.RecordImpressionsCountCall(pf, metadata)
}
