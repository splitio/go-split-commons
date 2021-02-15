package mocks

import "github.com/splitio/go-split-commons/v3/dtos"

// MockSegmentFetcher mocked implementation of segment fetcher
type MockSegmentFetcher struct {
	FetchCall func(name string, changeNumber int64) (*dtos.SegmentChangesDTO, error)
}

// Fetch mock
func (m MockSegmentFetcher) Fetch(name string, changeNumber int64) (*dtos.SegmentChangesDTO, error) {
	return m.FetchCall(name, changeNumber)
}
