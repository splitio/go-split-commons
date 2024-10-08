package mocks

import (
	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/service"
)

// MockSplitFetcher mocked implementation of split fetcher
type MockSplitFetcher struct {
	FetchCall func(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error)
}

// Fetch mock
func (m MockSplitFetcher) Fetch(fetchOptions *service.FlagRequestParams) (*dtos.SplitChangesDTO, error) {
	return m.FetchCall(fetchOptions)
}
