package datatypes

import (
	"fmt"
	"strconv"

	"strings"
)

const (
	MetadataDelimiter   = "+"
	PreReleaseDelimiter = "-"
	ValueDelimiter      = "."
)

type Semver struct {
	major      int64
	minor      int64
	patch      int64
	preRelease []string
	isStable   bool
	metadata   string
	version    string
}

func BuildSemver(version string) (*Semver, error) {
	if len(strings.TrimSpace(version)) == 0 {
		return nil, fmt.Errorf("unable to convert to semver, version cannot be empty")
	}
	metadata, vWithoutMetadata, err := processMetadata(version)
	if err != nil {
		return nil, err
	}
	preRelease, vWithoutPreRelease, err := processPreRelease(vWithoutMetadata)
	if err != nil {
		return nil, err
	}
	major, minor, patch, err := processComponents(vWithoutPreRelease)
	if err != nil {
		return nil, err
	}

	return &Semver{
		metadata:   metadata,
		preRelease: preRelease,
		major:      major,
		minor:      minor,
		patch:      patch,
		isStable:   len(preRelease) == 0,
		version:    version,
	}, nil
}

func processMetadata(version string) (string, string, error) {
	index, metadata := calculateIndexAndTrimString(version, MetadataDelimiter)
	if index == -1 {
		return "", version, nil
	}

	if len(metadata) == 0 {
		return "", "", fmt.Errorf("unable to convert to semver, incorrect pre release data")

	}
	return metadata, strings.TrimSpace(version[0:index]), nil
}

func processPreRelease(vWithoutMetadata string) ([]string, string, error) {
	index, preReleaseData := calculateIndexAndTrimString(vWithoutMetadata, PreReleaseDelimiter)
	if index == -1 {
		return nil, vWithoutMetadata, nil
	}

	preRelease := strings.Split(preReleaseData, ValueDelimiter)

	if len(preRelease) == 0 || isEmpty(preRelease) {
		return nil, "", fmt.Errorf("unable to convert to semver, incorrect pre release data")
	}
	return preRelease, strings.TrimSpace(vWithoutMetadata[0:index]), nil
}

func calculateIndexAndTrimString(stringToTrim string, delimiter string) (int, string) {
	index := strings.Index(stringToTrim, delimiter)
	if index == -1 {
		return index, ""
	}

	return index, strings.TrimSpace(stringToTrim[index+1:])
}

func isEmpty(preRelease []string) bool {
	for _, pr := range preRelease {
		if len(pr) == 0 {
			return true
		}
	}
	return false
}

func processComponents(version string) (int64, int64, int64, error) {
	vParts := strings.Split(version, ValueDelimiter)
	if len(vParts) != 3 {
		return 0, 0, 0, fmt.Errorf("unable to convert to semver, incorrect format: %s", version)
	}

	major, err := strconv.ParseInt(vParts[0], 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("unable to convert to semver, incorrect format: %s", version)
	}
	minor, err := strconv.ParseInt(vParts[1], 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("unable to convert to semver, incorrect format: %s", version)
	}
	patch, err := strconv.ParseInt(vParts[2], 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("unable to convert to semver, incorrect format: %s", version)
	}
	return major, minor, patch, nil
}

func (s *Semver) Compare(toCompare Semver) int {
	if s.version == toCompare.version {
		return 0
	}

	// Compare major, minor, and patch versions numerically
	compareResult := compareLongs(s.major, toCompare.major)
	if compareResult != 0 {
		return compareResult
	}

	compareResult = compareLongs(s.minor, toCompare.minor)
	if compareResult != 0 {
		return compareResult
	}

	compareResult = compareLongs(s.patch, toCompare.patch)
	if compareResult != 0 {
		return compareResult
	}

	if !s.isStable && toCompare.isStable {
		return -1
	} else if s.isStable && !toCompare.isStable {
		return 1
	}

	minLength := 0
	if len(s.preRelease) > len(toCompare.preRelease) {
		minLength = len(toCompare.preRelease)
	} else {
		minLength = len(s.preRelease)
	}
	// Compare pre-release versions lexically
	for i := 0; i < minLength; i++ {
		if s.preRelease[i] == toCompare.preRelease[i] {
			continue
		}
		if isNumeric(s.preRelease[i]) && isNumeric(toCompare.preRelease[i]) {
			preRelease1, err := strconv.ParseInt(s.preRelease[i], 10, 64)
			if err != nil {
				return -1
			}
			preRelease2, err := strconv.ParseInt(s.preRelease[i], 10, 64)
			if err != nil {
				return -1
			}
			if preRelease1 != preRelease2 {
				if preRelease1 < preRelease2 {
					return -1
				}
				return 1
			} else {
				return 0
			}
		}
		return strings.Compare(s.preRelease[i], toCompare.preRelease[i])
	}

	// Compare lengths of pre-release versions
	sPreReleaseLen := len(s.preRelease)
	toComparePreReleaseLen := len(toCompare.preRelease)

	return compareLongs(int64(sPreReleaseLen), int64(toComparePreReleaseLen))
}

func isNumeric(strNum string) bool {
	if len(strNum) == 0 {
		return false
	}
	_, err := strconv.ParseInt(strNum, 10, 0)
	return err == nil
}

func compareLongs(compare1 int64, compare2 int64) int {
	if compare1 != compare2 {
		if compare1 < compare2 {
			return -1
		}
		return 1
	}
	return 0
}
