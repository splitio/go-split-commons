package mocks

import (
	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-split-commons/v5/service"
)

// MockSegmentFetcher mocked implementation of segment fetcher
type MockSegmentFetcher struct {
	FetchCall func(name string, fetchOptions *service.SegmentRequestParams) (*dtos.SegmentChangesDTO, error)
}

// Fetch mock
func (m MockSegmentFetcher) Fetch(name string, fetchOptions *service.SegmentRequestParams) (*dtos.SegmentChangesDTO, error) {
	return m.FetchCall(name, fetchOptions)
}
