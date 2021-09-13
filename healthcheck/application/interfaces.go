package application

import (
	"time"

	"github.com/splitio/go-toolkit/v5/logging"
)

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

const (
	// Critical severity
	Critical = iota
	// Low severity
	Low
)

// MonitorInterface application monitor interface
type MonitorInterface interface {
	Start()
	Stop()
	GetHealthStatus() HealthDto
	NotifyEvent(counterType int)
	Reset(counterType int, value int)
}

// CounterInterface application counter interface
type CounterInterface interface {
	IsHealthy() HealthyResult
	NotifyEvent()
	Reset(value int) error
	GetType() int
	Start()
	Stop()
}

// HealthyResult description
type HealthyResult struct {
	Name       string
	Severity   int
	Healthy    bool
	LastHit    *time.Time
	ErrorCount int
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

// Config counter configuration
type Config struct {
	Name                     string
	CounterType              int
	Periodic                 bool
	TaskFunc                 func(l logging.LoggerInterface, c CounterInterface) error
	Period                   int
	MaxErrorsAllowedInPeriod int
	Severity                 int
}
