package dtos

// FlagSetValidatonError represents a flag set validaton error
type FlagSetValidatonError struct {
	Message string
}

// Error implements golang error interface
func (f FlagSetValidatonError) Error() string {
	return f.Message
}
