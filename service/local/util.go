package local

import (
	"regexp"

	"github.com/splitio/go-toolkit/v5/logging"
)

const (
	// SplitFileFormatClassic represents the file format of the standard split definition file <feature treatment>
	SplitFileFormatClassic = iota
	// SplitFileFormatJSON represents the file format of a JSON representation of split dtos
	SplitFileFormatJSON
	// SplitFileFormatYAML represents the file format of a YAML representation of split dtos
	SplitFileFormatYAML
)

func DefineFormat(splitFile string, logger logging.LoggerInterface) int {
	var r = regexp.MustCompile("(?i)(.yml$|.yaml$)")
	if r.MatchString(splitFile) {
		return SplitFileFormatYAML
	}
	r = regexp.MustCompile(`(?i)\.json$`)
	if r.MatchString(splitFile) {
		return SplitFileFormatJSON
	}
	logger.Warning("Localhost mode: .split mocks will be deprecated soon in favor of YAML files, which provide more targeting power. Take a look in our documentation.")
	return SplitFileFormatClassic
}
