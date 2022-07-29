package util

import "github.com/splitio/go-split-commons/v4/conf"

func ImpressionModeMapper(mode string) string {
	switch mode {
	case "d":
		return conf.ImpressionsModeDebug
	case "n":
		return conf.ImpressionsModeNone
	default:
		return conf.ImpressionsModeOptimized
	}
}

func ImpressionModeShortVersion(mode string) string {
	if mode == "" {
		return "o"
	}

	return string(mode[0])
}
