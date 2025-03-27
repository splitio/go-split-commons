package mocks

import (
	"github.com/splitio/go-split-commons/v6/dtos"
)

type MockUniqueKeysStorage struct {
	PushCall func(featureName string, key string)
	PopNCall func(bulkSize int64) dtos.Uniques
}

func (m MockUniqueKeysStorage) Push(featureName string, key string) {
	m.PushCall(featureName, key)
}

func (m MockUniqueKeysStorage) PopN(bulkSize int64) dtos.Uniques {
	return m.PopNCall(bulkSize)
}
