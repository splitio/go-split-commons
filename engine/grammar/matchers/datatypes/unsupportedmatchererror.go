package datatypes

// UnsupportedMatcherError represents a flag set validaton error
type UnsupportedMatcherError struct {
	Message string
}

// Error implements golang error interface
func (f UnsupportedMatcherError) Error() string {
	return f.Message
}
