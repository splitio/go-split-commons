package segment

import "sync"

type Errors struct {
	errorList    []error
	SegmentNames []string
	Error        func() string
	mutex        sync.Mutex
}

func NewErrors() Errors {
	return Errors{
		errorList:    make([]error, 0),
		SegmentNames: make([]string, 0),
	}
}

func (e Errors) PrintErrors() string {
	var errorsToPrint string
	for i := 0; i < len(e.errorList); i++ {
		errorsToPrint = errorsToPrint + "{" + e.SegmentNames[0] + ": " + e.errorList[i].Error() + "} "
	}
	return errorsToPrint
}

func (e Errors) addError(err error) error {
	e.mutex.Lock()
	e.errorList = append(e.errorList, err)
	e.mutex.Unlock()
	return nil
}

func (e Errors) addSegmentName(name string) error {
	e.mutex.Lock()
	e.SegmentNames = append(e.SegmentNames, name)
	e.mutex.Unlock()
	return nil
}
