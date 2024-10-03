package dtos

// Item represents a single item with a name.
type MemItem struct {
	Name string `json:"n"`
}

// Member represents a member with keys and a change number.
type Member struct {
	Keys         []MemItem `json:"k"`
	ChangeNumber int64     `json:"cn,omitempty"`
}

// MembershipsDTO groups my segments and large segments.
type MembershipsDTO struct {
	MySegments    Member `json:"ms"`
	LargeSegments Member `json:"ls"`
}
