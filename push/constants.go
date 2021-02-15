package push

const (
	workerStatusIdle = iota
	workerStatusRunning
	workerStatusShuttingDown
)

const (
	pushManagerStatusIdle = iota
	pushManagerStatusRunning
	pushManagerStatusShuttingDown
)
