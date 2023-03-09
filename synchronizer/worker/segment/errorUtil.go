package segment

import "sync"

type SegmentError struct {
	errors map[string]error
	mutex  sync.Mutex
}

func NewErrors() SegmentError {
	return SegmentError{
		errors: make(map[string]error),
		mutex:  sync.Mutex{},
	}
}

func (e *SegmentError) Error() string {
	errorsToPrint := ""
	for key, _ := range e.errors {
		errorsToPrint = errorsToPrint + "{" + key + ": " + e.errors[key].Error() + "} "
	}
	return errorsToPrint
}

func (e *SegmentError) addError(name string, err error) {
	e.mutex.Lock()
	e.errors[name] = err
	e.mutex.Unlock()
}
