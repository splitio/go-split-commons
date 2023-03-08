package segment

import (
	"fmt"
	"testing"
)

func TestCreateErrorUtil(t *testing.T) {

	errorTest := NewErrors()

	errorTest.addSegmentName("segment-1")
	err1 := fmt.Errorf("error for segment-1")
	errorTest.addError(err1)

	errorTest.addSegmentName("segment-2")
	err2 := fmt.Errorf("error for segment-2")
	errorTest.addError(err2)

	if len(errorTest.SegmentNames) != 2 {
		t.Error("the error size should be 2. it was: ", len(errorTest.SegmentNames))
	}

	if len(errorTest.ErrorList) != 2 {
		t.Error("the error size should be 2. it was: ", len(errorTest.ErrorList))
	}

	if errorTest.PrintErrors() != "{segment-1: error for segment-1} {segment-2: error for segment-2} " {
		t.Error("the printError was: ", errorTest.PrintErrors())
	}
}
