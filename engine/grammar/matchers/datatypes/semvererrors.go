package datatypes

// SemverValidatonError represents a semver validaton error
type SemverValidatonError struct {
	Message string
}

// Error implements golang error interface
func (f SemverValidatonError) Error() string {
	return f.Message
}
