package segment

import "sync"

type Errors struct {
	ErrorList    []error
	SegmentNames []string
	Error        func() string
	mutex        sync.Mutex
}

func NewErrors() Errors {
	return Errors{
		ErrorList:    make([]error, 0),
		SegmentNames: make([]string, 0),
	}
}

func (e *Errors) PrintErrors() string {
	var errorsToPrint string
	for i := 0; i < len(e.ErrorList); i++ {
		errorsToPrint = errorsToPrint + "{" + e.SegmentNames[i] + ": " + e.ErrorList[i].Error() + "} "
	}
	return errorsToPrint
}

func (e *Errors) addError(err error) error {
	e.mutex.Lock()
	e.ErrorList = append(e.ErrorList, err)
	e.mutex.Unlock()
	return nil
}

func (e *Errors) addSegmentName(name string) error {
	e.mutex.Lock()
	e.SegmentNames = append(e.SegmentNames, name)
	e.mutex.Unlock()
	return nil
}
