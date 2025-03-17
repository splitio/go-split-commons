package mocks

import (
	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/storage"
	"github.com/stretchr/testify/mock"
)

type MockUniqueKeysStorage struct {
	mock.Mock
}

func (u *MockUniqueKeysStorage) Add(featureName string, key string) {
	u.Called(featureName, key)
}

func (u *MockUniqueKeysStorage) PopAll() dtos.Uniques {
	args := u.Called()
	return args.Get(0).(dtos.Uniques)
}

var _ storage.UniqueKeysStorageConsumer = (*MockUniqueKeysStorage)(nil)
var _ storage.UniqueKeysStorageProducer = (*MockUniqueKeysStorage)(nil)
