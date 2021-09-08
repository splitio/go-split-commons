package application

import "time"

// MonitorInterface application monitor interface
type MonitorInterface interface {
	Start()
	Stop()
	GetHealthStatus() HealthDto
	NotifyEvent(counterType int)
	Reset(counterType int, value int)
}

// HealthDto struct
type HealthDto struct {
	Healthy      bool       `json:"healthy"`
	HealthySince *time.Time `json:"healthySince"`
	Items        []ItemDto  `json:"items"`
}

// ItemDto struct
type ItemDto struct {
	Name       string     `json:"name"`
	Healthy    bool       `json:"healthy"`
	LastHit    *time.Time `json:"lastHit,omitempty"`
	ErrorCount int        `json:"errorCount,omitempty"`
	Severity   int        `json:"-"`
}

const (
	// Splits counter type
	Splits = iota
	// Segments counter type
	Segments
	// Storage counter type
	Storage
	// SyncErros counter type
	SyncErros
)
