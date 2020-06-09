package mocks

import "github.com/splitio/go-split-commons/dtos"

// MockImpressionStorage is a mocked implementation of Impression Storage
type MockImpressionStorage struct {
	EmptyCall            func() bool
	LogImpressionsCall   func(impressions []dtos.Impression) error
	PopNCall             func(n int64) ([]dtos.Impression, error)
	PopNWithMetadataCall func(n int64) ([]dtos.ImpressionQueueObject, error)
}

// Empty mock
func (m MockImpressionStorage) Empty() bool {
	return m.EmptyCall()
}

// LogImpressions mock
func (m MockImpressionStorage) LogImpressions(impressions []dtos.Impression) error {
	return m.LogImpressionsCall(impressions)
}

// PopN mock
func (m MockImpressionStorage) PopN(n int64) ([]dtos.Impression, error) {
	return m.PopNCall(n)
}

// PopNWithMetadata mock
func (m MockImpressionStorage) PopNWithMetadata(n int64) ([]dtos.ImpressionQueueObject, error) {
	return m.PopNWithMetadataCall(n)
}
