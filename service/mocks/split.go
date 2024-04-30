package mocks

import (
	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-split-commons/v5/service"
)

// MockSplitFetcher mocked implementation of split fetcher
type MockSplitFetcher struct {
	FetchCall func(fetchOptions *service.SplitFetchOptions) (*dtos.SplitChangesDTO, error)
}

// Fetch mock
func (m MockSplitFetcher) Fetch(fetchOptions *service.SplitFetchOptions) (*dtos.SplitChangesDTO, error) {
	return m.FetchCall(fetchOptions)
}
