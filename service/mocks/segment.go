package mocks

import (
	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-split-commons/v5/service"
)

// MockSegmentFetcher mocked implementation of segment fetcher
type MockSegmentFetcher struct {
	FetchCall func(name string, changeNumber int64, fetchOptions *service.FetchOptions) (*dtos.SegmentChangesDTO, error)
}

// Fetch mock
func (m MockSegmentFetcher) Fetch(name string, changeNumber int64, fetchOptions *service.FetchOptions) (*dtos.SegmentChangesDTO, error) {
	return m.FetchCall(name, changeNumber, fetchOptions)
}
