package mocks

import "github.com/splitio/go-split-commons/dtos"

// MockSplitFetcher mocked implementation of split fetcher
type MockSplitFetcher struct {
	FetchCall func(changeNumber int64) (*dtos.SplitChangesDTO, error)
}

// Fetch mock
func (m MockSplitFetcher) Fetch(changeNumber int64) (*dtos.SplitChangesDTO, error) {
	return m.FetchCall(changeNumber)
}
