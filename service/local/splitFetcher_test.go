package local

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/splitio/go-split-commons/v4/service"
	"github.com/splitio/go-split-commons/v4/service/local/mocks"
	"github.com/splitio/go-toolkit/v5/logging"
)

var jsonTest_default = []byte(`{"splits":[],"since":-1,"till":-1}`)
var jsonTest_0 = []byte(`{"splits":[{"trafficTypeName":"user","name":"SPLIT_1","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]}],"since":-1,"till":-1}`)
var jsonTest = []byte(`{"splits":[{"trafficTypeName":"user","name":"SPLIT_1","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]}],"since":-1,"till":-2}`)

func TestLocalSplitFetcher(t *testing.T) {
	file, err := ioutil.TempFile("", "localhost_test")
	if err != nil {
		t.Error("should no fail to open temp file. Got: ", err)
	}
	defer os.Remove(file.Name())

	if _, err := file.Write([]byte("feature1 on\nfeature2 off\n")); err != nil {
		t.Error("writing to the file should not fail")
	}

	if err := file.Sync(); err != nil {
		t.Error("syncing the file should not fail")
	}

	logger := logging.NewLogger(nil)
	fetcher := NewFileSplitFetcher(file.Name(), logger)

	res, err := fetcher.Fetch(-1, &service.FetchOptions{})
	if err != nil {
		t.Error("fetching should not fail. Got: ", err)
	}

	if res.Since != -1 || res.Till != 0 {
		t.Error("Wrong since/till. Got: ", res.Since, res.Till)
	}

	if len(res.Splits) != 2 {
		t.Error("should have 2 splits. has: ", res.Splits)
	}

	// second call -- no change -- since == till

	res, err = fetcher.Fetch(0, &service.FetchOptions{})
	if err != nil {
		t.Error("fetching should not fail. Got: ", err)
	}

	if res.Since != 0 || res.Till != 0 {
		t.Error("Wrong since/till. Got: ", res.Since, res.Till)
	}

	if len(res.Splits) != 2 {
		t.Error("should have 2 splits. has: ", res.Splits)
	}

	if _, err := file.Write([]byte("feature3 yes\n")); err != nil {
		t.Error("writing to the file should not fail")
	}

	// third call -- change -- till > since

	res, err = fetcher.Fetch(0, &service.FetchOptions{})
	if err != nil {
		t.Error("fetching should not fail. Got: ", err)
	}

	if res.Since != 0 || res.Till != 1 {
		t.Error("Wrong since/till. Got: ", res.Since, res.Till)
	}

	if len(res.Splits) != 3 {
		t.Error("should have 2 splits. has: ", res.Splits)
	}

	// fourth call -- no change -- till != since

	res, err = fetcher.Fetch(1, &service.FetchOptions{})
	if err != nil {
		t.Error("fetching should not fail. Got: ", err)
	}

	if res.Since != 1 || res.Till != 1 {
		t.Error("Wrong since/till. Got: ", res.Since, res.Till)
	}

	if len(res.Splits) != 3 {
		t.Error("should have 2 splits. has: ", res.Splits)
	}
}

func TestLocalSplitFetcherJson(t *testing.T) {
	logger := logging.NewLogger(nil)

	fetcher := NewFileSplitFetcher("../../testdata/splitChange_mock.json", logger)

	res, err := fetcher.Fetch(-1, &service.FetchOptions{})
	if err != nil {
		t.Error("fetching should not fail. Got: ", err)
	}

	if res.Since != 1660326991072 || res.Till != 1660326991072 {
		t.Error("Wrong since/till. Got: ", res.Since, res.Till)
	}

	if len(res.Splits) != 7 {
		t.Error("should have 7 splits. has: ", res.Splits)
	}

	if res.Splits[0].Name != "split_1" {
		t.Error("DTO mal formed")
	}

	if res.Splits[0].Configurations == nil {
		t.Error("DTO mal formed")
	}
}

func TestLocalSplitFetcherJsonTest1(t *testing.T) {
	file, err := ioutil.TempFile("", "localhost_test-*.json")
	if err != nil {
		t.Error("should no fail to open temp file. Got: ", err)
	}
	defer os.Remove(file.Name())

	if _, err := file.Write(jsonTest); err != nil {
		t.Error("writing to the file should not fail")
	}

	if err := file.Sync(); err != nil {
		t.Error("syncing the file should not fail")
	}

	logger := logging.NewLogger(nil)
	fetcher := NewFileSplitFetcher(file.Name(), logger)

	_, err = fetcher.Fetch(-1, &service.FetchOptions{})

	if err == nil {
		t.Error("fetching should fail.")
	}
}

func TestFetchSomeSplits(t *testing.T) {
	fetches := 0
	mockedFetchers := FileSegmentFetcher{
		reader: mocks.MockReader{
			ReadFileCall: func(filename string) ([]byte, error) {
				if filename != "/test_1.json" {
					t.Error("Cannot construct path")
				}
				switch fetches {
				case 0:
					return jsonTest_0, nil
				}
				return jsonTest_default, nil
			},
		},
		logger:   &logging.Logger{},
		lastHash: make(map[string][]byte),
	}
	fetches++
	// 1) The CN from storage is -1, till and since are -1, and sha is different than before. It's going to return a segment change with updates.
	splitChange, _ := mockedFetchers.Fetch("test_1", -1, nil)
	if splitChange.Since != -1 || splitChange.Till != -1 {
		t.Error("Wrong since/till. Got: ", splitChange.Since, splitChange.Till)
	}
	if splitChange.Added[0] != "user-1" {
		t.Error("DTO mal formed")
	}
	if splitChange.Removed[0] != "user-2" {
		t.Error("DTO mal formed")
	}
}
