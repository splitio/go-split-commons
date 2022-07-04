package synchronizer

import (
	"github.com/splitio/go-toolkit/v5/logging"
)

// ManagerImpl struct
type ManagerRedisImpl struct {
	synchronizer Synchronizer
	logger       logging.LoggerInterface
}

// NewSynchronizerManagerRedis creates new sync manager for redis
func NewSynchronizerManagerRedis(
	synchronizer Synchronizer,
	logger logging.LoggerInterface,
) Manager {
	return &ManagerRedisImpl{
		synchronizer: synchronizer,
		logger:       logger,
	}
}

func (m *ManagerRedisImpl) Start() {
	m.synchronizer.StartPeriodicDataRecording()
}

func (m *ManagerRedisImpl) Stop() {
	m.synchronizer.StopPeriodicDataRecording()
}

func (m *ManagerRedisImpl) IsRunning() bool {
	return true
}
