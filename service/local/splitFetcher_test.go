package local

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/splitio/go-split-commons/v7/service"
	"github.com/splitio/go-split-commons/v7/service/local/mocks"
	"github.com/splitio/go-toolkit/v5/logging"
)

var jsonTest_default = []byte(`{"splits":[],"since":-1,"till":-1}`)
var jsonTest_0 = []byte(`{"splits":[{"trafficTypeName":"user","name":"SPLIT_1","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]}],"since":-1,"till":-1}`)
var jsonTest_1 = []byte(`{"splits":[{"trafficTypeName":"user","name":"SPLIT_1","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]},{"trafficTypeName":"user","name":"SPLIT_2","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]}],"since":-1,"till":-1}`)
var jsonTest_2 = []byte(`{"splits":[{"trafficTypeName":"user","name":"SPLIT_1","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]},{"trafficTypeName":"user","name":"SPLIT_2","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]}],"since":-1,"till":2323}`)
var jsonTest_3 = []byte(`{"splits":[{"trafficTypeName":"user","name":"SPLIT_1","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]}],"since":-1,"till":2323}`)
var jsonTest_4 = []byte(`{"splits":[{"trafficTypeName":"user","name":"SPLIT_1","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]}],"since":-1,"till":445345}`)
var jsonTest_5 = []byte(`{"splits":[{"trafficTypeName":"user","name":"SPLIT_1","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]},{"trafficTypeName":"user","name":"SPLIT_2","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]}],"since":-1,"till":-1}`)
var jsonTest = []byte(`{"splits":[{"trafficTypeName":"user","name":"SPLIT_1","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]}],"since":-1,"till":-2}`)
var jsonSplitWithoutName = []byte(`{"splits":[{"trafficTypeName":"user","name":"SPLIT_1","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]},{"trafficTypeName":"user","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]}],"since":-1,"till":-1}`)
var jsonSplitMatcherEmpty = []byte(`{"splits":[{"trafficTypeName":"user","name":"SPLIT_1","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{}}],"since":-1,"till":-1}`)
var jsonSplitSanitization = []byte(`{"splits":[{"trafficTypeName":"user","name":"SPLIT_1","trafficAllocation":1000,"status":"ACTIVE","killed":false,"changeNumber":1675443537882,"algo":9,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]}],"since":-1,"till":2323}`)

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
	fetcher := NewFileSplitFetcher(file.Name(), logger, SplitFileFormatClassic)

	res, err := fetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(-1))
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

	res, err = fetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(0))
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

	res, err = fetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(0))
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

	res, err = fetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(1))
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

	fetcher := NewFileSplitFetcher("../../testdata/splitChange_mock.json", logger, SplitFileFormatJSON)

	res, err := fetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(-1))
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
	fetcher := NewFileSplitFetcher(file.Name(), logger, SplitFileFormatJSON)

	_, err = fetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(-1))

	if err != nil {
		t.Error("fetching should not fail.")
	}
}

func TestFetchSomeSplits(t *testing.T) {
	fetches := 0
	mockedFetchers := FileSplitFetcher{
		reader: mocks.MockReader{
			ReadFileCall: func(filename string) ([]byte, error) {
				if filename != "" {
					t.Error("Cannot construct path")
				}
				switch fetches {
				case 0:
					return jsonTest_0, nil
				case 1:
					return jsonTest_1, nil
				case 2:
					return jsonTest_2, nil
				case 3:
					return jsonTest_3, nil
				case 4:
					return jsonTest_4, nil
				case 5:
					return jsonTest_5, nil
				}
				return jsonTest_default, nil
			},
		},
		logger:     &logging.Logger{},
		fileFormat: SplitFileFormatJSON,
	}
	// 0) The CN from storage is -1, till and since are -1, and sha doesn't exist in the hash. It's going to return a split change with updates.
	splitChange, _ := mockedFetchers.Fetch(service.MakeFlagRequestParams().WithChangeNumber(-1))
	if splitChange.Since != -1 || splitChange.Till != -1 {
		t.Error("Wrong since/till. Got: ", splitChange.Since, splitChange.Till)
	}
	if len(splitChange.Splits) != 1 {
		t.Error("should have 1 split. has: ", splitChange.Splits)
	}

	fetches++
	// 1) The CN from storage is -1, till and since are -1, and sha is different than before. It's going to return a split change with updates.
	splitChange, _ = mockedFetchers.Fetch(service.MakeFlagRequestParams().WithChangeNumber(-1))
	if splitChange.Since != -1 || splitChange.Till != -1 {
		t.Error("Wrong since/till. Got: ", splitChange.Since, splitChange.Till)
	}
	if len(splitChange.Splits) != 2 {
		t.Error("should have 1 split. has: ", splitChange.Splits)
	}

	fetches++
	// 2) The CN from storage is -1, till is 2323, and since is -1, and sha is the same as before. It's going to return a split change with the same data.
	splitChange, _ = mockedFetchers.Fetch(service.MakeFlagRequestParams().WithChangeNumber(-1))
	if splitChange.Since != -1 || splitChange.Till != -1 {
		t.Error("Wrong since/till. Got: ", splitChange.Since, splitChange.Till)
	}
	if len(splitChange.Splits) != 2 {
		t.Error("should have 2 splits. has: ", splitChange.Splits)
	}

	fetches++
	// 3) The CN from storage is -1, till is 2323, and since is -1, sha is different than before. It's going to return a split change with updates.
	splitChange, _ = mockedFetchers.Fetch(service.MakeFlagRequestParams().WithChangeNumber(-1))
	if splitChange.Since != 2323 || splitChange.Till != 2323 {
		t.Error("Wrong since/till. Got: ", splitChange.Since, splitChange.Till)
	}
	if len(splitChange.Splits) != 1 {
		t.Error("should have 1 split. has: ", splitChange.Splits)
	}

	fetches++
	// 4) The CN from storage is 2323, till is 445345, and since is -1, and sha is the same as before. It's going to return a split change with same data.
	splitChange, _ = mockedFetchers.Fetch(service.MakeFlagRequestParams().WithChangeNumber(2323))
	if splitChange.Since != 2323 || splitChange.Till != 2323 {
		t.Error("Wrong since/till. Got: ", splitChange.Since, splitChange.Till)
	}
	if len(splitChange.Splits) != 1 {
		t.Error("should have 1 split. has: ", splitChange.Splits)
	}

	fetches++
	// 5) The CN from storage is 2323, till and since are -1, and sha is different than before. It's going to return a split change with updates.
	splitChange, _ = mockedFetchers.Fetch(service.MakeFlagRequestParams().WithChangeNumber(2323))
	if splitChange.Since != 2323 || splitChange.Till != 2323 {
		t.Error("Wrong since/till. Got: ", splitChange.Since, splitChange.Till)
	}
	if len(splitChange.Splits) != 2 {
		t.Error("should have 2 splits. has: ", splitChange.Splits)
	}
}

func TestSplitWithoutName(t *testing.T) {
	file, err := ioutil.TempFile("", "localhost_test-*.json")
	if err != nil {
		t.Error("should no fail to open temp file. Got: ", err)
	}
	defer os.Remove(file.Name())

	if _, err := file.Write(jsonSplitWithoutName); err != nil {
		t.Error("writing to the file should not fail")
	}

	if err := file.Sync(); err != nil {
		t.Error("syncing the file should not fail")
	}

	logger := logging.NewLogger(nil)
	fetcher := NewFileSplitFetcher(file.Name(), logger, SplitFileFormatJSON)

	res, err := fetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(-1))

	if err != nil {
		t.Error("fetching should not fail. Got: ", err)
	}

	if len(res.Splits) != 1 {
		t.Error("Should has one split. Got: ", len(res.Splits))
	}
}

func TestSplitMatchersEmpty(t *testing.T) {
	file, err := ioutil.TempFile("", "localhost_test-*.json")
	if err != nil {
		t.Error("should no fail to open temp file. Got: ", err)
	}
	defer os.Remove(file.Name())

	if _, err := file.Write(jsonSplitMatcherEmpty); err != nil {
		t.Error("writing to the file should not fail")
	}

	if err := file.Sync(); err != nil {
		t.Error("syncing the file should not fail")
	}

	logger := logging.NewLogger(nil)
	fetcher := NewFileSplitFetcher(file.Name(), logger, SplitFileFormatJSON)

	res, err := fetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(-1))

	if err != nil {
		t.Error("fetching should not fail. Got: ", err)
	}

	if len(res.Splits) != 1 {
		t.Error("should has one split. Got: ", len(res.Splits))
	}

	split := res.Splits[0]
	if split.Conditions[0].MatcherGroup.Matchers[0].MatcherType != "ALL_KEYS" {
		t.Error("the matcher type should be all keys. Got: ", split.Conditions[0].MatcherGroup.Matchers[0].MatcherType)
	}

	if split.Conditions[0].MatcherGroup.Matchers[0].KeySelector.TrafficType != "user" {
		t.Error("the matcher type should be user. Got: ", split.Conditions[0].MatcherGroup.Matchers[0].KeySelector.TrafficType)
	}
}

func TestSplitSanitization(t *testing.T) {
	file, err := ioutil.TempFile("", "localhost_test-*.json")
	if err != nil {
		t.Error("should no fail to open temp file. Got: ", err)
	}
	defer os.Remove(file.Name())

	if _, err := file.Write(jsonSplitSanitization); err != nil {
		t.Error("writing to the file should not fail")
	}

	if err := file.Sync(); err != nil {
		t.Error("syncing the file should not fail")
	}

	logger := logging.NewLogger(nil)
	fetcher := NewFileSplitFetcher(file.Name(), logger, SplitFileFormatJSON)

	res, err := fetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(-1))

	if err != nil {
		t.Error("fetching should not fail. Got: ", err)
	}

	if res.Splits[0].Algo != 2 {
		t.Error("algo should be 2. Got: ", res.Splits[0].Algo)
	}

	if res.Splits[0].TrafficAllocation != 100 {
		t.Error("traffic allocation should be 100. Got: ", res.Splits[0].TrafficAllocation)
	}

	if res.Splits[0].TrafficAllocationSeed == 0 {
		t.Error("traffic allocation seed can't be 0")
	}

	if res.Splits[0].Seed == 0 {
		t.Error("seed can't be 0")
	}

	if res.Splits[0].DefaultTreatment != "control" {
		t.Error("default treatment should be control")
	}
}
