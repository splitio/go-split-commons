package dtos

// SegmentChangesDTO struct to map a segment change message.
type SegmentChangesDTO struct {
	Name    string   `json:"name"`
	Added   []string `json:"added"`
	Removed []string `json:"removed"`
	Since   int64    `json:"since"`
	Till    int64    `json:"till"`
}

// SegmentKeyDTO maps key data
type SegmentKeyDTO struct {
	Name         string
	LastModified int64
	Removed      bool
}

// MySegmentDTO struct mapping segment data for mySegments endpoint
type MySegmentDTO struct {
	Name string `json:"name"`
}
