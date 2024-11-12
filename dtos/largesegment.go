package dtos

import "net/http"

// Params
type Params struct {
	Method  string      `json:"m"`           // method
	URL     string      `json:"u"`           // url
	Headers http.Header `json:"h"`           // headers
	Body    []byte      `json:"b,omitempty"` // body
}

// Data
type Data struct {
	Interval  *int64 `json:"i,omitempty"` // interval
	Format    int    `json:"f"`           // 0 unknown | 1 csv | otro // format
	TotalKeys int64  `json:"k"`           // totalKeys
	FileSize  int64  `json:"s"`           // fileSize
	ExpiresAt int64  `json:"e"`           // expiration time url
}

// RFD struct
type RFD struct {
	Data   Data   `json:"d"`
	Params Params `json:"p"`
}

// LargeSegmentRFDResponseDTO
type LargeSegmentRFDResponseDTO struct {
	NotificationType string `json:"t"`
	RFD              *RFD   `json:"rfd,omitempty"`
	SpecVersion      string `json:"v"`
	ChangeNumber     int64  `json:"cn"`
}

// LargeSegment
type LargeSegment struct {
	Name         string
	Keys         []string
	ChangeNumber int64
}
