package push

import "github.com/splitio/go-toolkit/v5/datautils"

type DataUtils interface {
	Decode(data string) ([]byte, error)
	Decompress(data []byte, compressType int) ([]byte, error)
}

type DataUtilsImpl struct {
}

func NewDataUtilsImpl() DataUtils {
	return &DataUtilsImpl{}
}

func (d *DataUtilsImpl) Decode(data string) ([]byte, error) {
	return datautils.Decode(data, datautils.Base64)
}
func (d *DataUtilsImpl) Decompress(data []byte, compressType int) ([]byte, error) {
	return datautils.Decompress(data, compressType)
}
