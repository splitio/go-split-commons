package local

import (
	"testing"

	"github.com/splitio/go-toolkit/v5/logging"
)

func TestDefineFormat(t *testing.T) {
	logger := logging.NewLogger(nil)
	jsonFile := "test.json"
	format := DefineFormat(jsonFile, logger)
	if format != SplitFileFormatJSON {
		t.Error("file format badly defined")
	}

	jsonFile = "test.yaml"
	format = DefineFormat(jsonFile, logger)
	if format != SplitFileFormatYAML {
		t.Error("file format badly defined")
	}

	jsonFile = "test"
	format = DefineFormat(jsonFile, logger)
	if format != SplitFileFormatClassic {
		t.Error("file format badly defined")
	}
}
