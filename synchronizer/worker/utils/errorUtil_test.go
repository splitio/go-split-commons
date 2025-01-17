package utils

import (
	"fmt"
	"strings"
	"testing"
)

func TestCreateErrorUtil(t *testing.T) {

	errorTest := NewErrors()

	err1 := fmt.Errorf("error for segment-1")
	errorTest.AddError("segment-1", err1)

	err2 := fmt.Errorf("error for segment-2")
	errorTest.AddError("segment-2", err2)

	if len(errorTest.errors) != 2 {
		t.Error("the error size should be 2. it was: ", len(errorTest.errors))
	}

	if !strings.Contains(errorTest.Error(), "segment-1") {
		t.Error("the printError was: ", errorTest.Error())
	}

	if !strings.Contains(errorTest.Error(), "segment-2") {
		t.Error("the printError was: ", errorTest.Error())
	}
}
