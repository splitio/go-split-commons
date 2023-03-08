package segment

import (
	"fmt"
	"testing"
)

func TestCreateErrorUtil(t *testing.T) {

	errorTest := NewErrors()

	err1 := fmt.Errorf("error for segment-1")
	errorTest.addError("segment-1", err1)

	err2 := fmt.Errorf("error for segment-2")
	errorTest.addError("segment-2", err2)

	if len(errorTest.ErrorMap) != 2 {
		t.Error("the error size should be 2. it was: ", len(errorTest.ErrorMap))
	}

	if errorTest.PrintErrors() != "{segment-1: error for segment-1} {segment-2: error for segment-2} " {
		t.Error("the printError was: ", errorTest.PrintErrors())
	}
}
