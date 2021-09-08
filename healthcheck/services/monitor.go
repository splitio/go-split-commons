package services

import "time"

// MonitorInterface services monitor interface
type MonitorInterface interface {
	Start()
	Stop()
	GetHealthStatus() HealthDto
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

const (
	// ByPercentage counter type
	ByPercentage = iota
	// Secuencial counter type
	Secuencial
)
