package util

import (
	"testing"

	"github.com/splitio/go-split-commons/v4/conf"
)

func TestMapper(t *testing.T) {
	if conf.ImpressionsModeOptimized != ImpressionModeMapper("") {
		t.Errorf("Should be %s", conf.ImpressionsModeOptimized)
	}

	if conf.ImpressionsModeOptimized != ImpressionModeMapper("o") {
		t.Errorf("Should be %s", conf.ImpressionsModeOptimized)
	}

	if conf.ImpressionsModeDebug != ImpressionModeMapper("d") {
		t.Errorf("Should be %s", conf.ImpressionsModeDebug)
	}

	if conf.ImpressionsModeNone != ImpressionModeMapper("n") {
		t.Errorf("Should be %s", conf.ImpressionsModeNone)
	}
}

func TestShortVersion(t *testing.T) {
	if "o" != ImpressionModeShortVersion("") {
		t.Error("Should be o")
	}

	if "o" != ImpressionModeShortVersion(conf.ImpressionsModeOptimized) {
		t.Error("Should be o")
	}

	if "d" != ImpressionModeShortVersion(conf.ImpressionsModeDebug) {
		t.Error("Should be d")
	}

	if "n" != ImpressionModeShortVersion(conf.ImpressionsModeNone) {
		t.Error("Should be n")
	}
}
