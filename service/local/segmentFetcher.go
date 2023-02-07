package local

import (
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/splitio/go-split-commons/v4/dtos"
	"github.com/splitio/go-split-commons/v4/service"
	"github.com/splitio/go-toolkit/v5/logging"
)

// FileSegmentFetcher struct fetches segments from a file
type FileSegmentFetcher struct {
	segmentDirectory string
	addedLastHash    []byte
	removedLastHash  []byte
	logger           logging.LoggerInterface
}

// NewFileSegmentFetcher returns a new instance of LocalFileSegmentFetcher
func NewFileSegmentFetcher(segmentDirectory string, logger logging.LoggerInterface) service.SegmentFetcher {
	return &FileSegmentFetcher{
		segmentDirectory: segmentDirectory,
		logger:           logger,
	}
}

func (f *FileSegmentFetcher) parseSegmentJson(data string) *dtos.SegmentChangesDTO {
	var segmentChangesDto dtos.SegmentChangesDTO
	err := json.Unmarshal([]byte(data), &segmentChangesDto)
	if err != nil {
		f.logger.Error(fmt.Sprintf("error: %v", err))
		return &segmentChangesDto
	}
	return &segmentChangesDto
}

func (s *FileSegmentFetcher) processSegmentJson(data string, changeNumber int64) *dtos.SegmentChangesDTO {
	var segmentChangesDto dtos.SegmentChangesDTO
	segmentChangesDto.Since = changeNumber
	segmentChangesDto.Till = changeNumber
	segmentChange := s.parseSegmentJson(data)
	addedJson, err := json.Marshal(segmentChange.Added)
	if err != nil {
		s.logger.Error(fmt.Sprintf("error: %v", err))
		return &segmentChangesDto
	}
	removedJson, err := json.Marshal(segmentChange.Removed)
	if err != nil {
		s.logger.Error(fmt.Sprintf("error: %v", err))
		return &segmentChangesDto
	}

	addedCurrH := sha1.New()
	addedCurrH.Write(addedJson)
	addedCurrSum := addedCurrH.Sum(nil)
	removedCurrH := sha1.New()

	removedCurrH.Write(removedJson)
	removedCurrSum := removedCurrH.Sum(nil)
	if bytes.Equal(addedCurrSum, s.addedLastHash) || bytes.Equal(removedCurrSum, s.removedLastHash) || segmentChange.Till < changeNumber {
		return &segmentChangesDto
	}
	s.addedLastHash = addedCurrSum
	s.removedLastHash = removedCurrSum
	segmentChange.Since = segmentChange.Till
	segmentChange.Till++
	return segmentChange
}

// Fetch parses the file and returns the appropriate structures
func (s *FileSegmentFetcher) Fetch(segmentName string, changeNumber int64, _ *service.FetchOptions) (*dtos.SegmentChangesDTO, error) {
	fileContents, err := ioutil.ReadFile(fmt.Sprintf("%v/%v.json", s.segmentDirectory, segmentName))
	if err != nil {
		return nil, err
	}
	data := string(fileContents)
	segmentChange := s.processSegmentJson(data, changeNumber)
	return segmentChange, nil
}
