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
	lastHash         map[string][]byte
	logger           logging.LoggerInterface
}

// NewFileSegmentFetcher returns a new instance of LocalFileSegmentFetcher
func NewFileSegmentFetcher(segmentDirectory string, logger logging.LoggerInterface) service.SegmentFetcher {
	return &FileSegmentFetcher{
		segmentDirectory: segmentDirectory,
		lastHash:         make(map[string][]byte),
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

func (s *FileSegmentFetcher) processSegmentJson(segmentName string, data string, changeNumber int64) (*dtos.SegmentChangesDTO, error) {
	var segmentChangesDto dtos.SegmentChangesDTO
	segmentChangesDto.Since = changeNumber
	segmentChangesDto.Till = changeNumber
	segmentChange := s.parseSegmentJson(data)
	addedJson, err := json.Marshal(segmentChange.Added)
	if err != nil {
		s.logger.Error(fmt.Sprintf("error: %v", err))
		return &segmentChangesDto, nil
	}
	removedJson, err := json.Marshal(segmentChange.Removed)
	if err != nil {
		s.logger.Error(fmt.Sprintf("error: %v", err))
		return &segmentChangesDto, nil
	}

	jsonToSha := append(addedJson, removedJson...)

	currH := sha1.New()
	currH.Write(jsonToSha)
	currSum := currH.Sum(nil)

	if lastHash, ok := s.lastHash[segmentName]; ok {
		if bytes.Equal(currSum, lastHash) {
			return &segmentChangesDto, nil
		}
	}
	if segmentChange.Till < changeNumber {
		return &segmentChangesDto, nil
	}
	s.lastHash[segmentName] = currSum
	segmentChange.Since = segmentChange.Till
	segmentChange.Till++
	return segmentChange, nil
}

// Fetch parses the file and returns the appropriate structures
func (s *FileSegmentFetcher) Fetch(segmentName string, changeNumber int64, _ *service.FetchOptions) (*dtos.SegmentChangesDTO, error) {
	fileContents, err := ioutil.ReadFile(fmt.Sprintf("%v/%v.json", s.segmentDirectory, segmentName))
	if err != nil {
		return nil, err
	}
	data := string(fileContents)
	return s.processSegmentJson(segmentName, data, changeNumber)
}
