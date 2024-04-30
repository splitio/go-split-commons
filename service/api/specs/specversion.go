package specs

const (
	FLAG_V1_0 = "1.0"
	FLAG_V1_1 = "1.1"
)

// Match returns the spec version if it is valid, otherwise it returns nil
func Match(specVersion string) *string {
	if specVersion == FLAG_V1_0 || specVersion == FLAG_V1_1 {
		return &specVersion
	}
	return nil
}
