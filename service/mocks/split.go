package mocks

import (
	"github.com/splitio/go-split-commons/v7/dtos"
	"github.com/splitio/go-split-commons/v7/service"

	"github.com/stretchr/testify/mock"
)

// MockSplitFetcher mocked implementation of split fetcher
type MockSplitFetcher struct {
	mock.Mock
}

// Fetch mock
func (m *MockSplitFetcher) Fetch(fetchOptions *service.FlagRequestParams) (dtos.FFResponse, error) {
	args := m.Called(fetchOptions)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(dtos.FFResponse), args.Error(1)
}

// Fetch mock
func (m *MockSplitFetcher) IsProxy(fetchOptions *service.FlagRequestParams) bool {
	args := m.Called(fetchOptions)
	return args.Get(0).(bool)
}

var _ service.SplitFetcher = (*MockSplitFetcher)(nil)
