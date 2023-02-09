package local

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/splitio/go-split-commons/v4/dtos"
	"github.com/splitio/go-split-commons/v4/service"
	"github.com/splitio/go-split-commons/v4/service/local/mocks"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestLocalSegmentFetcherJson(t *testing.T) {

	logger := logging.NewLogger(nil)

	fetcher := NewFileSegmentFetcher("../../testdata", logger)

	res, err := fetcher.Fetch("segment_mock", -1, &service.FetchOptions{})
	if err != nil {
		t.Error("fetching should not fail. Got: ", err)
	}

	if res.Since != 1489542661161 || res.Till != 1489542661161 {
		t.Error("Wrong since/till. Got: ", res.Since, res.Till)
	}

	if res.Name != "employees" {
		t.Error("should name be employees. is: ", res.Name)
	}

	if res.Added[0] != "user_for_testing_do_no_erase" {
		t.Error("DTO mal formed")
	}

	if len(res.Removed) != 0 {
		t.Error("DTO mal formed")
	}
}

func TestInvalidTill(t *testing.T) {
	logger := logging.NewLogger(nil)

	fetcher := NewFileSegmentFetcher("../../testdata", logger)

	_, err := fetcher.Fetch("segmentTillInvalid", -1, &service.FetchOptions{})
	if err == nil {
		t.Error("fetching should not fail. Got: ", err)
	}
}

func TestFetchSomeSegments(t *testing.T) {
	fetches := 0
	mockedFetchers := FileSegmentFetcher{
		reader: mocks.MockReader{
			ReadFileCall: func(filename string) ([]byte, error) {
				if filename != "/test_1.json" {
					t.Error("Cannot construct path")
				}
				switch fetches {
				case 0:
					segmentChange := dtos.SegmentChangesDTO{
						Added:   []string{"user-1"},
						Removed: make([]string, 0),
						Till:    -1,
						Since:   -1,
					}
					asJson, _ := json.Marshal(segmentChange)
					return asJson, nil
				case 1:
					segmentChange := dtos.SegmentChangesDTO{
						Added:   []string{"user-1"},
						Removed: []string{"user-2"},
						Till:    -1,
						Since:   -1,
					}
					asJson, _ := json.Marshal(segmentChange)
					return asJson, nil
				case 2:
					segmentChange := dtos.SegmentChangesDTO{
						Added:   []string{"user-1"},
						Removed: []string{"user-2"},
						Till:    2323,
						Since:   -1,
					}
					asJson, _ := json.Marshal(segmentChange)
					return asJson, nil
				case 3:
					segmentChange := dtos.SegmentChangesDTO{
						Added:   []string{"user-1", "user-3"},
						Removed: []string{"user-2"},
						Till:    2323,
						Since:   -1,
					}
					asJson, _ := json.Marshal(segmentChange)
					return asJson, nil
				case 4:
					segmentChange := dtos.SegmentChangesDTO{
						Added:   []string{"user-1", "user-3"},
						Removed: []string{"user-2"},
						Till:    445345,
						Since:   -1,
					}
					asJson, _ := json.Marshal(segmentChange)
					return asJson, nil
				case 5:
					segmentChange := dtos.SegmentChangesDTO{
						Added:   []string{"user-1"},
						Removed: []string{"user-2", "user-3"},
						Till:    -1,
						Since:   -1,
					}
					asJson, _ := json.Marshal(segmentChange)
					return asJson, nil
				}
				segmentChange := dtos.SegmentChangesDTO{
					Added:   make([]string, 0),
					Removed: make([]string, 0),
					Till:    -1,
					Since:   -1,
				}
				return []byte(fmt.Sprintf("%v", segmentChange)), nil
			},
		},
		logger:   &logging.Logger{},
		lastHash: make(map[string][]byte),
	}

	// 0) The CN from storage is -1, till and since are -1, and sha doesn't exist in the hash. It's going to return a segment change with updates.
	segmentChange, _ := mockedFetchers.Fetch("test_1", -1, nil)
	if segmentChange.Since != -1 || segmentChange.Till != -1 {
		t.Error("Wrong since/till. Got: ", segmentChange.Since, segmentChange.Till)
	}
	if segmentChange.Added[0] != "user-1" {
		t.Error("DTO mal formed")
	}
	if len(segmentChange.Removed) != 0 {
		t.Error("DTO mal formed")
	}

	fetches++
	// 1) The CN from storage is -1, till and since are -1, and sha is different than before. It's going to return a segment change with updates.
	segmentChange, _ = mockedFetchers.Fetch("test_1", -1, nil)
	if segmentChange.Since != -1 || segmentChange.Till != -1 {
		t.Error("Wrong since/till. Got: ", segmentChange.Since, segmentChange.Till)
	}
	if segmentChange.Added[0] != "user-1" {
		t.Error("DTO mal formed")
	}
	if segmentChange.Removed[0] != "user-2" {
		t.Error("DTO mal formed")
	}

	fetches++
	// 2) The CN from storage is -1, till is 2323, and since is -1, and sha is the same as before. It's going to return a segment change with the same data.
	segmentChange, _ = mockedFetchers.Fetch("test_1", -1, nil)
	if segmentChange.Since != -1 || segmentChange.Till != -1 {
		t.Error("Wrong since/till. Got: ", segmentChange.Since, segmentChange.Till)
	}
	if segmentChange.Added[0] != "user-1" {
		t.Error("DTO mal formed")
	}
	if segmentChange.Removed[0] != "user-2" {
		t.Error("DTO mal formed")
	}

	fetches++
	// 3) The CN from storage is -1, till is 2323, and since is -1, sha is different than before. It's going to return a segment change with updates.
	segmentChange, _ = mockedFetchers.Fetch("test_1", -1, nil)
	if segmentChange.Since != 2323 || segmentChange.Till != 2323 {
		t.Error("Wrong since/till. Got: ", segmentChange.Since, segmentChange.Till)
	}
	if segmentChange.Added[0] != "user-1" && segmentChange.Added[1] != "user-3" {
		t.Error("DTO mal formed")
	}
	if segmentChange.Removed[0] != "user-2" {
		t.Error("DTO mal formed")
	}

	fetches++
	// 4) The CN from storage is -1, till is 445345, and since is -1, and sha is the same as before. It's going to return a segment change with same data.
	segmentChange, _ = mockedFetchers.Fetch("test_1", 2323, nil)
	if segmentChange.Since != 2323 || segmentChange.Till != 2323 {
		t.Error("Wrong since/till. Got: ", segmentChange.Since, segmentChange.Till)
	}
	if segmentChange.Added[0] != "user-1" && segmentChange.Added[1] != "user-3" {
		t.Error("DTO mal formed")
	}
	if segmentChange.Removed[0] != "user-2" {
		t.Error("DTO mal formed")
	}

	fetches++
	// 5) The CN from storage is -1, till and since are -1, and sha is different than before. It's going to return a segment change with updates.
	segmentChange, _ = mockedFetchers.Fetch("test_1", 2323, nil)
	if segmentChange.Since != 2323 || segmentChange.Till != 2323 {
		t.Error("Wrong since/till. Got: ", segmentChange.Since, segmentChange.Till)
	}
	if segmentChange.Added[0] != "user-1" {
		t.Error("DTO mal formed")
	}
	if segmentChange.Removed[0] != "user-2" && segmentChange.Removed[1] != "user-3" {
		t.Error("DTO mal formed")
	}
}
