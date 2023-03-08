package segment

import "sync"

type Errors struct {
	ErrorMap map[string]error
	Error    func() string
	mutex    sync.Mutex
}

func NewErrors() Errors {
	return Errors{
		ErrorMap: make(map[string]error),
	}
}

func (e *Errors) PrintErrors() string {
	var errorsToPrint string
	for key, _ := range e.ErrorMap {
		errorsToPrint = errorsToPrint + "{" + key + ": " + e.ErrorMap[key].Error() + "} "
	}
	return errorsToPrint
}

func (e *Errors) addError(name string, err error) error {
	e.mutex.Lock()
	e.ErrorMap[name] = err
	e.mutex.Unlock()
	return nil
}
