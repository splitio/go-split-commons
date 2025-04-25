package utils

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
	e.mutex.Lock()
	defer e.mutex.Unlock()
	for key := range e.errors {
		errorsToPrint = errorsToPrint + "{" + key + ": " + e.errors[key].Error() + "} "
	}
	return errorsToPrint
}

func (e *SegmentError) AddError(name string, err error) {
	e.mutex.Lock()
	e.errors[name] = err
	e.mutex.Unlock()
}
