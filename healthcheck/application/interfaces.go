package application

import (
	"time"

	"github.com/splitio/go-toolkit/v5/logging"
)

const (
	// Splits monitor type
	Splits = iota
	// Segments monitor type
	Segments
	// Storage monitor type
	Storage
	// SyncErros monitor type
	SyncErros
)

const (
	// Periodic counter type
	Periodic = iota
	// Threshold counter type
	Threshold
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
	NotifyEvent(monitorType int)
	Reset(monitorType int, value int)
}

// CounterInterface application counter interface
type CounterInterface interface {
	IsHealthy() HealthyResult
	NotifyEvent()
	Reset(value int) error
	GetMonitorType() int
	UpdateLastHit()
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
	MonitorType              int
	TaskFunc                 func(l logging.LoggerInterface, c CounterInterface) error
	Period                   int
	MaxErrorsAllowedInPeriod int
	Severity                 int
}

// NewApplicationConfig new config with default values
func NewApplicationConfig(
	name string,
	monitorType int,
) *Config {
	return &Config{
		Name:        name,
		MonitorType: monitorType,
		CounterType: Threshold,
		Period:      3600,
		Severity:    Critical,
	}
}
