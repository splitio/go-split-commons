package mocks

type MockReader struct {
	ReadFileCall func(filename string) ([]byte, error)
}

func (m MockReader) ReadFile(filename string) ([]byte, error) {
	return m.ReadFileCall(filename)
}
