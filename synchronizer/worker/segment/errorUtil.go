package segment

type Errors struct {
	errorList    []error
	segmentNames []string
	Error        func() string
}

func NewErrors(errorList []error, segments []string) Errors {
	return Errors{
		segmentNames: segments,
		errorList:    errorList,
	}
}

func (e Errors) PrintErrors() string {
	var errorsToPrint string
	for i := 0; i < len(e.errorList); i++ {
		errorsToPrint = errorsToPrint + "{" + e.segmentNames[0] + ": " + e.errorList[i].Error() + "} "
	}
	return errorsToPrint
}
