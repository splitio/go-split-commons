package synchronizer

// ManagerImpl struct
type ManagerRedisImpl struct {
	synchronizer Synchronizer
}

// NewSynchronizerManagerRedis creates new sync manager for redis
func NewSynchronizerManagerRedis(synchronizer Synchronizer) Manager {
	return &ManagerRedisImpl{
		synchronizer: synchronizer,
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
