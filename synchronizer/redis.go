package synchronizer

import (
	"sync"
	"sync/atomic"

	"github.com/splitio/go-toolkit/v5/logging"
)

// ManagerImpl struct
type ManagerRedisImpl struct {
	synchronizer Synchronizer
	mutex        *sync.RWMutex
	running      atomic.Value
	logger       logging.LoggerInterface
}

// NewSynchronizerManagerRedis creates new sync manager for redis
func NewSynchronizerManagerRedis(synchronizer Synchronizer, logger logging.LoggerInterface) Manager {
	manager := &ManagerRedisImpl{
		synchronizer: synchronizer,
		mutex:        &sync.RWMutex{},
		running:      atomic.Value{},
		logger:       logger,
	}

	manager.running.Store(false)

	return manager
}

func (m *ManagerRedisImpl) Start() {
	if !m.running.CompareAndSwap(false, true) {
		m.logger.Info("Manager is already running, skipping start")
		return
	}
	m.synchronizer.StartPeriodicDataRecording()
}

func (m *ManagerRedisImpl) Stop() {
	if !m.running.CompareAndSwap(true, false) {
		m.logger.Info("sync manager not yet running, skipping shutdown.")
		return
	}
	m.synchronizer.StopPeriodicDataRecording()
}

func (m *ManagerRedisImpl) IsRunning() bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	return m.running.Load().(bool)
}
