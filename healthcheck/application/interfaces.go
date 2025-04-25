package application

const (
	// Splits monitor type
	Splits = iota
	// Segments monitor type
	Segments
	// Storage monitor type
	Storage
	// SyncErros monitor type
	SyncErros
	// LargeSegments monitor type
	LargeSegments
)

// MonitorProducerInterface application monitor producer interface
type MonitorProducerInterface interface {
	NotifyEvent(monitorType int)
	Reset(monitorType int, value int)
}
