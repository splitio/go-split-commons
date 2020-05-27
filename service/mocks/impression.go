package mocks

import "github.com/splitio/go-split-commons/dtos"

// MockImpressionRecorder mocked implementation of impression recorder
type MockImpressionRecorder struct {
	RecordCall func(impressions []dtos.Impression) error
}

// Record mock
func (m MockImpressionRecorder) Record(impressions []dtos.Impression) error {
	return m.RecordCall(impressions)
}
