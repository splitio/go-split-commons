package mocks

import (
	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-split-commons/v5/service"
)

// MockSplitFetcher mocked implementation of split fetcher
type MockSplitFetcher struct {
	FetchCall func(changeNumber int64, fetchOptions *service.FetchOptions) (*dtos.SplitChangesDTO, error)
}

// Fetch mock
func (m MockSplitFetcher) Fetch(changeNumber int64, fetchOptions *service.FetchOptions) (*dtos.SplitChangesDTO, error) {
	return m.FetchCall(changeNumber, fetchOptions)
}
