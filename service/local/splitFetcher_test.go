package local

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/splitio/go-split-commons/v4/service"
	"github.com/splitio/go-toolkit/v5/logging"
)

var jsonTest_1 = []byte(`{"splits":[{"trafficTypeName":"user","name":"SPLIT_1","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]}],"since":-1,"till":-2}`)

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

	if res.Since != 1660326991072 || res.Till != 1660326991073 {
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

	if _, err := file.Write(jsonTest_1); err != nil {
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

	if res.Since != -1 || res.Till != -1 {
		t.Error("Wrong since/till. Got: ", res.Since, res.Till)
	}

	if len(res.Splits) != 0 {
		t.Error("should have 0 split. has: ", res.Splits)
	}
}
