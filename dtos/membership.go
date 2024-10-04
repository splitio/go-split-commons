package dtos

// Segment struct
type Segment struct {
	Name string `json:"n"`
}

// Memberships struct mapping segments data for memberships endpoint
type Memberships struct {
	Segments     []Segment `json:"k"`
	ChangeNumber *int64    `json:"cn,omitempty"`
}

// MembershipsResponseDTO struct mapping memberships data for memberships endpoint
type MembershipsResponseDTO struct {
	MySegments      Memberships `json:"ms"`
	MyLargeSegments Memberships `json:"ls"`
}
