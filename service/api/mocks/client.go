package mocks

// ClientMock mocks client
type ClientMock struct {
	GetCall  func(service string, headers map[string]string) ([]byte, error)
	PostCall func(service string, body []byte, headers map[string]string) error
}

// Get mocks Get
func (c ClientMock) Get(service string, headers map[string]string) ([]byte, error) {
	return c.GetCall(service, headers)
}

// Post mocks Post
func (c ClientMock) Post(service string, body []byte, headers map[string]string) error {
	return c.PostCall(service, body, headers)
}
