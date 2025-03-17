package mocks

import (
	"github.com/splitio/go-split-commons/v6/dtos"
)

type MockUniqueKeysStorage struct {
	AddCall    func(featureName string, key string)
	PopAllCall func() dtos.Uniques
}

func (m MockUniqueKeysStorage) Add(featureName string, key string) {
	m.AddCall(featureName, key)
}

func (m MockUniqueKeysStorage) PopAll() dtos.Uniques {
	return m.PopAllCall()
}
