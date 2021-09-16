package services

import "time"

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
	Service      string     `json:"service"`
	Healthy      bool       `json:"healthy"`
	Message      string     `json:"message,omitempty"`
	HealthySince *time.Time `json:"healthySince,omitempty"`
	LastHit      *time.Time `json:"lastHit,omitempty"`
}

// HealthyResult result
type HealthyResult struct {
	Name         string
	Severity     int
	Healthy      bool
	LastMessage  string
	HealthySince *time.Time
	LastHit      *time.Time
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
	TaskPeriod            int
}

// NewServicesConfig new config with default values
func NewServicesConfig(
	name string,
	url string,
	endpoint string,
) *Config {
	return &Config{
		CounterType:           ByPercentage,
		MaxLen:                10,
		PercentageToBeHealthy: 70,
		Name:                  name,
		ServiceURL:            url,
		TaskPeriod:            3600,
		ServiceHealthEndpoint: endpoint,
		Severity:              Critical,
	}
}
