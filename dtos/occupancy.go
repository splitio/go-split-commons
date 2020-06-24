package dtos

// Metrics dto
type Metrics struct {
	Publishers int `json:"publishers"`
}

// Occupancy dto
type Occupancy struct {
	Data Metrics `json:"metrics"`
}
