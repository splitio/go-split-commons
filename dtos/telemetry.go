package dtos

// LastSynchronization struct
type LastSynchronization struct {
	Splits      int64 `json:"sp,omitempty"`
	Segments    int64 `json:"se,omitempty"`
	Impressions int64 `json:"im,omitempty"`
	Events      int64 `json:"ev,omitempty"`
	Token       int64 `json:"to,omitempty"`
	Telemetry   int64 `json:"te,omitempty"`
}

// HTTPErrors struct
type HTTPErrors struct {
	Splits      map[int]int64 `json:"sp,omitempty"`
	Segments    map[int]int64 `json:"se,omitempty"`
	Impressions map[int]int64 `json:"im,omitempty"`
	Events      map[int]int64 `json:"ev,omitempty"`
	Token       map[int]int64 `json:"to,omitempty"`
	Telemetry   map[int]int64 `json:"te,omitempty"`
}

// HTTPLatencies struct
type HTTPLatencies struct {
	Splits      []int64 `json:"sp,omitempty"`
	Segments    []int64 `json:"se,omitempty"`
	Impressions []int64 `json:"im,omitempty"`
	Events      []int64 `json:"ev,omitempty"`
	Token       []int64 `json:"to,omitempty"`
	Telemetry   []int64 `json:"te,omitempty"`
}

// MethodLatencies struct
type MethodLatencies struct {
	Treatment            []int64 `json:"t,omitempty"`
	Treatments           []int64 `json:"ts,omitempty"`
	TreatmentWithConfig  []int64 `json:"tc,omitempty"`
	TreatmentWithConfigs []int64 `json:"tcs,omitempty"`
	Track                []int64 `json:"tr,omitempty"`
}

// MethodExceptions struct
type MethodExceptions struct {
	Treatment            int64 `json:"t,omitempty"`
	Treatments           int64 `json:"ts,omitempty"`
	TreatmentWithConfig  int64 `json:"tc,omitempty"`
	TreatmentWithConfigs int64 `json:"tcs,omitempty"`
	Track                int64 `json:"tr,omitempty"`
}

// StreamingEvent struct
type StreamingEvent struct {
	Type      int   `json:"e,omitempty"`
	Data      int64 `json:"d,omitempty"`
	Timestamp int64 `json:"t,omitempty"`
}
