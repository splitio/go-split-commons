package services

import (
	"github.com/splitio/go-toolkit/v5/logging"
)

const (
	// ByPercentage counter type
	ByPercentage = iota
	// Sequential counter type
	Sequential
)

const (
	// Critical severity
	Critical = iota
	// Degraded severity
	Degraded
	// Low severity
	Low
)

// MonitorInterface services monitor interface
type MonitorInterface interface {
	Start()
	Stop()
	GetHealthStatus() HealthDto
}

// CounterInterface interface
type CounterInterface interface {
	NotifyServiceHit(statusCode int, message string)
	IsHealthy() HealthyResult
	Start()
	Stop()
}

// HealthDto description
type HealthDto struct {
	Status string    `json:"serviceStatus"`
	Items  []ItemDto `json:"dependencies"`
}

// ItemDto description
type ItemDto struct {
	Service      string `json:"service"`
	Healthy      bool   `json:"healthy"`
	Message      string `json:"message,omitempty"`
	HealthySince *int64 `json:"healthySince,omitempty"`
	LastHit      *int64 `json:"lastHit,omitempty"`
}

// HealthyResult result
type HealthyResult struct {
	Name         string
	Severity     int
	Healthy      bool
	LastMessage  string
	HealthySince *int64
	LastHit      *int64
}

// Config counter config
type Config struct {
	CounterType           int
	MaxErrorsAllowed      int
	MinSuccessExpected    int
	MaxLen                int
	PercentageToBeHealthy int
	Name                  string
	ServiceURL            string
	ServiceHealthEndpoint string
	Severity              int
	TaskFunc              func(l logging.LoggerInterface, c CounterInterface) error
	TaskPeriod            int
}
