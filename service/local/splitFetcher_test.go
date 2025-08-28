package local

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/splitio/go-split-commons/v6/service"
	"github.com/splitio/go-split-commons/v6/service/local/mocks"
	"github.com/splitio/go-toolkit/v5/logging"
)

var jsonTest_default = []byte(`{"ff":{"s":-1,"t":-1,"d":[]}}`)
var jsonTest_0 = []byte(`{"ff":{"d":[{"trafficTypeName":"user","name":"SPLIT_1","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]}],"s":-1,"t":-1}}`)
var jsonTest_1 = []byte(`{"ff":{"d":[{"trafficTypeName":"user","name":"SPLIT_1","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]},{"trafficTypeName":"user","name":"SPLIT_2","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]}],"s":-1,"t":-1}}`)
var jsonTest_2 = []byte(`{"ff":{"d":[{"trafficTypeName":"user","name":"SPLIT_1","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]},{"trafficTypeName":"user","name":"SPLIT_2","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]}],"s":-1,"t":2323}}`)
var jsonTest_3 = []byte(`{"ff":{"d":[{"trafficTypeName":"user","name":"SPLIT_1","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]}],"s":-1,"t":2323}}`)
var jsonTest_4 = []byte(`{"ff":{"d":[{"trafficTypeName":"user","name":"SPLIT_1","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]}],"s":-1,"t":445345}}`)
var jsonTest_5 = []byte(`{"ff":{"d":[{"trafficTypeName":"user","name":"SPLIT_1","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]},{"trafficTypeName":"user","name":"SPLIT_2","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]}],"s":-1,"t":-1}}`)
var jsonTest = []byte(`{"ff":{"d":[{"trafficTypeName":"user","name":"SPLIT_1","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]}],"s":-1,"t":-2}}`)
var jsonSplitWithoutName = []byte(`{"ff":{"d":[{"trafficTypeName":"user","name":"SPLIT_1","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]},{"trafficTypeName":"user","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]}],"s":-1,"t":-1}}`)
var jsonSplitMatcherEmpty = []byte(`{"ff":{"d":[{"trafficTypeName":"user","name":"SPLIT_1","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{}}],"s":-1,"t":-1}}`)
var jsonSplitWithRuleBasedSegment = []byte(`{"ff":{"d":[{"trafficTypeName":"user","name":"SPLIT_WITH_RULE","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"IN_SEGMENT","negate":false,"userDefinedSegmentMatcherData":{"segmentName":"test-segment"},"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":100}],"label":"rule based segment"}]}],"s":-1,"t":-1},"rbs":{"d":[{"name":"test-segment","trafficTypeName":"user","status":"ACTIVE","changeNumber":1675443537882,"conditions":[{"matcherGroup":{"combiner":"AND","matchers":[{"matcherType":"ALL_KEYS","negate":false}]}}]}],"s":-1,"t":-1}}`)

var jsonSplitSanitization = []byte(`{"ff":{"d":[{"trafficTypeName":"user","name":"SPLIT_1","trafficAllocation":1000,"status":"ACTIVE","killed":false,"changeNumber":1675443537882,"algo":9,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]}],"s":-1,"t":2323}}`)

var jsonSplitWithOldTill = []byte(`{"ff":{"d":[{"trafficTypeName":"user","name":"SPLIT_1","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"ALL_KEYS","negate":false,"userDefinedSegmentMatcherData":null,"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":0},{"treatment":"off","size":100}],"label":"default rule"}]}],"s":1,"t":50}}`)

var jsonSplitWithOldRBSTill = []byte(`{"ff":{"d":[{"trafficTypeName":"user","name":"SPLIT_WITH_RULE","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"IN_SEGMENT","negate":false,"userDefinedSegmentMatcherData":{"segmentName":"test-segment"},"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":100}],"label":"rule based segment"}]}],"s":-1,"t":-1},"rbs":{"d":[{"name":"test-segment","trafficTypeName":"user","status":"ACTIVE","changeNumber":1675443537882,"conditions":[{"matcherGroup":{"combiner":"AND","matchers":[{"matcherType":"ALL_KEYS","negate":false}]}}]}],"s":50,"t":50}}`)

var jsonSplitWithRBSDefaultTill = []byte(`{"ff":{"d":[{"trafficTypeName":"user","name":"SPLIT_WITH_RULE","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"IN_SEGMENT","negate":false,"userDefinedSegmentMatcherData":{"segmentName":"test-segment"},"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":100}],"label":"rule based segment"}]}],"s":-1,"t":-1},"rbs":{"d":[{"name":"test-segment","trafficTypeName":"user","status":"ACTIVE","changeNumber":1675443537882,"conditions":[{"matcherGroup":{"combiner":"AND","matchers":[{"matcherType":"ALL_KEYS","negate":false}]}}]}],"s":25,"t":25}}`)

var jsonSplitWithRBSEmptySegments = []byte(`{"ff":{"d":[{"trafficTypeName":"user","name":"SPLIT_WITH_RULE","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"IN_SEGMENT","negate":false,"userDefinedSegmentMatcherData":{"segmentName":"test-segment"},"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":100}],"label":"rule based segment"}]}],"s":-1,"t":-1},"rbs":{"d":[],"s":50,"t":50}}`)

var jsonSplitWithRBSNilSegments = []byte(`{"ff":{"d":[{"trafficTypeName":"user","name":"SPLIT_WITH_RULE","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"IN_SEGMENT","negate":false,"userDefinedSegmentMatcherData":{"segmentName":"test-segment"},"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":100}],"label":"rule based segment"}]}],"s":-1,"t":-1},"rbs":{"d":null,"s":50,"t":50}}`)

var jsonSplitWithRBSDifferentSegments = []byte(`{"ff":{"d":[{"trafficTypeName":"user","name":"SPLIT_WITH_RULE","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"IN_SEGMENT","negate":false,"userDefinedSegmentMatcherData":{"segmentName":"test-segment"},"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":100}],"label":"rule based segment"}]}],"s":-1,"t":-1},"rbs":{"d":[{"name":"test-segment","trafficTypeName":"user","status":"ACTIVE","changeNumber":1675443537882,"conditions":[{"matcherGroup":{"combiner":"AND","matchers":[{"matcherType":"ALL_KEYS","negate":true}]}}]}],"s":50,"t":50}}`)

var jsonSplitWithRBSNoHash = []byte(`{"ff":{"d":[{"trafficTypeName":"user","name":"SPLIT_WITH_RULE","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"IN_SEGMENT","negate":false,"userDefinedSegmentMatcherData":{"segmentName":"test-segment"},"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":100}],"label":"rule based segment"}]}],"s":-1,"t":-1},"rbs":{"d":[{"name":"test-segment","trafficTypeName":"user","status":"ACTIVE","changeNumber":1675443537882,"conditions":[{"matcherGroup":{"combiner":"AND","matchers":[{"matcherType":"ALL_KEYS","negate":false}]}}]}],"s":50,"t":50}}`)

var jsonSplitWithRBSHashMatch = []byte(`{"ff":{"d":[{"trafficTypeName":"user","name":"SPLIT_WITH_RULE","trafficAllocation":100,"trafficAllocationSeed":-1780071202,"seed":-1442762199,"status":"ACTIVE","killed":false,"defaultTreatment":"off","changeNumber":1675443537882,"algo":2,"configurations":{},"conditions":[{"conditionType":"ROLLOUT","matcherGroup":{"combiner":"AND","matchers":[{"keySelector":{"trafficType":"user","attribute":null},"matcherType":"IN_SEGMENT","negate":false,"userDefinedSegmentMatcherData":{"segmentName":"test-segment"},"whitelistMatcherData":null,"unaryNumericMatcherData":null,"betweenMatcherData":null,"booleanMatcherData":null,"dependencyMatcherData":null,"stringMatcherData":null}]},"partitions":[{"treatment":"on","size":100}],"label":"rule based segment"}]}],"s":-1,"t":-1},"rbs":{"d":[{"name":"test-segment","trafficTypeName":"user","status":"ACTIVE","changeNumber":1675443537882,"conditions":[{"matcherGroup":{"combiner":"AND","matchers":[{"matcherType":"ALL_KEYS","negate":false}]}}]}],"s":50,"t":50}}`)

func TestProcessSplitJson(t *testing.T) {
	file, err := ioutil.TempFile("", "localhost_test-*.json")
	if err != nil {
		t.Error("should not fail to open temp file. Got: ", err)
	}
	defer os.Remove(file.Name())

	// Test case 1: RBS with defaultTill (-1)
	if _, err := file.Write(jsonSplitWithRBSDefaultTill); err != nil {
		t.Error("writing to the file should not fail")
	}

	if err := file.Sync(); err != nil {
		t.Error("syncing the file should not fail")
	}

	logger := logging.NewLogger(nil)
	fetcher := NewFileSplitFetcher(file.Name(), logger, SplitFileFormatJSON)

	// First fetch - RBS has defaultTill (-1)
	res, err := fetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(25))
	if err != nil {
		t.Error("Expected no error, got:", err)
		return
	}

	// Since RBS has defaultTill, both since/till should be updated to changeNumber
	if res.RuleBasedSegments.Since != 25 || res.RuleBasedSegments.Till != 25 {
		t.Error("Expected RBS since/till to be 25 (change number), got:", res.RuleBasedSegments.Since, res.RuleBasedSegments.Till)
	}

	// Test case 2: RBS with different hash and till = 50
	if err := ioutil.WriteFile(file.Name(), jsonSplitWithRBSNoHash, 0644); err != nil {
		t.Error("writing to the file should not fail")
	}

	// Fetch with change number = 25 (less than till)
	res, err = fetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(25))
	if err != nil {
		t.Error("Expected no error when change number is less than till, got:", err)
		return
	}

	// Since RBS has different hash and till > changeNumber, since should be updated to till
	if res.RuleBasedSegments.Since != 50 || res.RuleBasedSegments.Till != 50 {
		t.Error("Expected RBS since/till to be 50/50, got:", res.RuleBasedSegments.Since, res.RuleBasedSegments.Till)
	}

	// Test case 3: RBS with same hash and till = 50
	if err := ioutil.WriteFile(file.Name(), jsonSplitWithRBSHashMatch, 0644); err != nil {
		t.Error("writing to the file should not fail")
	}

	// Fetch with change number = 75 (greater than till)
	res, err = fetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(75))
	if err == nil {
		t.Error("Expected error when change number is greater than till")
		return
	}

	// Test case 4: RBS with same hash and till = 50
	if err := ioutil.WriteFile(file.Name(), jsonSplitWithRBSHashMatch, 0644); err != nil {
		t.Error("writing to the file should not fail")
	}

	// Fetch with change number = 50 (equal to till)
	res, err = fetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(50))
	if err != nil {
		t.Error("Expected no error when change number equals till, got:", err)
		return
	}

	// Since RBS has same hash and till = changeNumber, since should be updated to till
	if res.RuleBasedSegments.Since != 50 || res.RuleBasedSegments.Till != 50 {
		t.Error("Expected RBS since/till to be 50/50, got:", res.RuleBasedSegments.Since, res.RuleBasedSegments.Till)
	}

	// Test case 5: RBS with empty segments
	if err := ioutil.WriteFile(file.Name(), jsonSplitWithRBSEmptySegments, 0644); err != nil {
		t.Error("writing to the file should not fail")
	}

	// Fetch with change number = 25 (less than till)
	res, err = fetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(25))
	if err != nil {
		t.Error("Expected no error when change number is less than till, got:", err)
		return
	}

	// Since RBS has empty segments, since should be updated to till
	if res.RuleBasedSegments.Since != 50 || res.RuleBasedSegments.Till != 50 {
		t.Error("Expected RBS since/till to be 50/50, got:", res.RuleBasedSegments.Since, res.RuleBasedSegments.Till)
	}

	// Test case 6: RBS with nil segments
	if err := ioutil.WriteFile(file.Name(), jsonSplitWithRBSNilSegments, 0644); err != nil {
		t.Error("writing to the file should not fail")
	}

	// Fetch with change number = 25 (less than till)
	res, err = fetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(25))
	if err != nil {
		t.Error("Expected no error when change number is less than till, got:", err)
		return
	}

	// Since RBS has nil segments, since should be updated to till
	if res.RuleBasedSegments.Since != 50 || res.RuleBasedSegments.Till != 50 {
		t.Error("Expected RBS since/till to be 50/50, got:", res.RuleBasedSegments.Since, res.RuleBasedSegments.Till)
	}

	// Test case 7: RBS with different segments
	if err := ioutil.WriteFile(file.Name(), jsonSplitWithRBSDifferentSegments, 0644); err != nil {
		t.Error("writing to the file should not fail")
	}

	// Fetch with change number = 25 (less than till)
	res, err = fetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(25))
	if err != nil {
		t.Error("Expected no error when change number is less than till, got:", err)
		return
	}

	// Since RBS has different segments, since should be updated to till
	if res.RuleBasedSegments.Since != 50 || res.RuleBasedSegments.Till != 50 {
		t.Error("Expected RBS since/till to be 50/50, got:", res.RuleBasedSegments.Since, res.RuleBasedSegments.Till)
	}
}

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

	if res.FeatureFlags.Since != -1 || res.FeatureFlags.Till != 0 {
		t.Error("Wrong since/till. Got: ", res.FeatureFlags.Since, res.FeatureFlags.Till)
	}

	if len(res.FeatureFlags.Splits) != 2 {
		t.Error("should have 2 splits. has: ", res.FeatureFlags.Splits)
	}

	// second call -- no change -- since == till

	res, err = fetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(0))
	if err != nil {
		t.Error("fetching should not fail. Got: ", err)
	}

	if res.FeatureFlags.Since != 0 || res.FeatureFlags.Till != 0 {
		t.Error("Wrong since/till. Got: ", res.FeatureFlags.Since, res.FeatureFlags.Till)
	}

	if len(res.FeatureFlags.Splits) != 2 {
		t.Error("should have 2 splits. has: ", res.FeatureFlags.Splits)
	}

	if _, err := file.Write([]byte("feature3 yes\n")); err != nil {
		t.Error("writing to the file should not fail")
	}

	// third call -- change -- till > since

	res, err = fetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(0))
	if err != nil {
		t.Error("fetching should not fail. Got: ", err)
	}

	if res.FeatureFlags.Since != 0 || res.FeatureFlags.Till != 1 {
		t.Error("Wrong since/till. Got: ", res.FeatureFlags.Since, res.FeatureFlags.Till)
	}

	if len(res.FeatureFlags.Splits) != 3 {
		t.Error("should have 2 splits. has: ", res.FeatureFlags.Splits)
	}

	// fourth call -- no change -- till != since

	res, err = fetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(1))
	if err != nil {
		t.Error("fetching should not fail. Got: ", err)
	}

	if res.FeatureFlags.Since != 1 || res.FeatureFlags.Till != 1 {
		t.Error("Wrong since/till. Got: ", res.FeatureFlags.Since, res.FeatureFlags.Till)
	}

	if len(res.FeatureFlags.Splits) != 3 {
		t.Error("should have 2 splits. has: ", res.FeatureFlags.Splits)
	}
}

func TestLocalSplitFetcherJson(t *testing.T) {
	logger := logging.NewLogger(nil)

	fetcher := NewFileSplitFetcher("../../testdata/splitChange_mock.json", logger, SplitFileFormatJSON)

	res, err := fetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(-1))
	if err != nil {
		t.Error("fetching should not fail. Got: ", err)
	}

	if res.FeatureFlags.Since != 1660326991072 || res.FeatureFlags.Till != 1660326991072 {
		t.Error("Wrong since/till. Got: ", res.FeatureFlags.Since, res.FeatureFlags.Till)
	}

	if len(res.FeatureFlags.Splits) != 7 {
		t.Error("should have 7 splits. has: ", res.FeatureFlags.Splits)
	}

	if res.FeatureFlags.Splits[0].Name != "split_1" {
		t.Error("DTO mal formed")
	}

	if res.FeatureFlags.Splits[0].Configurations == nil {
		t.Error("DTO mal formed")
	}
}

func TestLocalSplitFetcherJsonWithRbs(t *testing.T) {
	logger := logging.NewLogger(nil)

	fetcher := NewFileSplitFetcher("../../testdata/splitChange_mock_1.json", logger, SplitFileFormatJSON)

	res, err := fetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(-1))
	if err != nil {
		t.Error("fetching should not fail. Got: ", err)
	}

	if res.FeatureFlags.Since != 10 || res.FeatureFlags.Till != 10 {
		t.Error("Wrong since/till. Got: ", res.FeatureFlags.Since, res.FeatureFlags.Till)
	}

	if len(res.FeatureFlags.Splits) != 1 {
		t.Error("should have 1 splits. has: ", res.FeatureFlags.Splits)
	}

	if res.FeatureFlags.Splits[0].Name != "rbs_split" {
		t.Error("DTO mal formed")
	}

	if res.FeatureFlags.Splits[0].Configurations == nil {
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
	if splitChange.FeatureFlags.Since != -1 || splitChange.FeatureFlags.Till != -1 {
		t.Error("Wrong since/till. Got: ", splitChange.FeatureFlags.Since, splitChange.FeatureFlags.Till)
	}
	if len(splitChange.FeatureFlags.Splits) != 1 {
		t.Error("should have 1 split. has: ", splitChange.FeatureFlags.Splits)
	}

	fetches++
	// 1) The CN from storage is -1, till and since are -1, and sha is different than before. It's going to return a split change with updates.
	splitChange, _ = mockedFetchers.Fetch(service.MakeFlagRequestParams().WithChangeNumber(-1))
	if splitChange.FeatureFlags.Since != -1 || splitChange.FeatureFlags.Till != -1 {
		t.Error("Wrong since/till. Got: ", splitChange.FeatureFlags.Since, splitChange.FeatureFlags.Till)
	}
	if len(splitChange.FeatureFlags.Splits) != 2 {
		t.Error("should have 1 split. has: ", splitChange.FeatureFlags.Splits)
	}

	fetches++
	// 2) The CN from storage is -1, till is 2323, and since is -1, and sha is the same as before. It's going to return a split change with the same data.
	splitChange, _ = mockedFetchers.Fetch(service.MakeFlagRequestParams().WithChangeNumber(-1))
	if splitChange.FeatureFlags.Since != -1 || splitChange.FeatureFlags.Till != -1 {
		t.Error("Wrong since/till. Got: ", splitChange.FeatureFlags.Since, splitChange.FeatureFlags.Till)
	}
	if len(splitChange.FeatureFlags.Splits) != 2 {
		t.Error("should have 2 splits. has: ", splitChange.FeatureFlags.Splits)
	}

	fetches++
	// 3) The CN from storage is -1, till is 2323, and since is -1, sha is different than before. It's going to return a split change with updates.
	splitChange, _ = mockedFetchers.Fetch(service.MakeFlagRequestParams().WithChangeNumber(-1))
	if splitChange.FeatureFlags.Since != 2323 || splitChange.FeatureFlags.Till != 2323 {
		t.Error("Wrong since/till. Got: ", splitChange.FeatureFlags.Since, splitChange.FeatureFlags.Till)
	}
	if len(splitChange.FeatureFlags.Splits) != 1 {
		t.Error("should have 1 split. has: ", splitChange.FeatureFlags.Splits)
	}

	fetches++
	// 4) The CN from storage is 2323, till is 445345, and since is -1, and sha is the same as before. It's going to return a split change with same data.
	splitChange, _ = mockedFetchers.Fetch(service.MakeFlagRequestParams().WithChangeNumber(2323))
	if splitChange.FeatureFlags.Since != 2323 || splitChange.FeatureFlags.Till != 2323 {
		t.Error("Wrong since/till. Got: ", splitChange.FeatureFlags.Since, splitChange.FeatureFlags.Till)
	}
	if len(splitChange.FeatureFlags.Splits) != 1 {
		t.Error("should have 1 split. has: ", splitChange.FeatureFlags.Splits)
	}

	fetches++
	// 5) The CN from storage is 2323, till and since are -1, and sha is different than before. It's going to return a split change with updates.
	splitChange, _ = mockedFetchers.Fetch(service.MakeFlagRequestParams().WithChangeNumber(2323))
	if splitChange.FeatureFlags.Since != 2323 || splitChange.FeatureFlags.Till != 2323 {
		t.Error("Wrong since/till. Got: ", splitChange.FeatureFlags.Since, splitChange.FeatureFlags.Till)
	}
	if len(splitChange.FeatureFlags.Splits) != 2 {
		t.Error("should have 2 splits. has: ", splitChange.FeatureFlags.Splits)
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

	if len(res.FeatureFlags.Splits) != 1 {
		t.Error("Should has one split. Got: ", len(res.FeatureFlags.Splits))
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

	if len(res.FeatureFlags.Splits) != 1 {
		t.Error("should has one split. Got: ", len(res.FeatureFlags.Splits))
	}

	split := res.FeatureFlags.Splits[0]
	if split.Conditions[0].MatcherGroup.Matchers[0].MatcherType != "ALL_KEYS" {
		t.Error("the matcher type should be all keys. Got: ", split.Conditions[0].MatcherGroup.Matchers[0].MatcherType)
	}

	if split.Conditions[0].MatcherGroup.Matchers[0].KeySelector.TrafficType != "user" {
		t.Error("the matcher type should be user. Got: ", split.Conditions[0].MatcherGroup.Matchers[0].KeySelector.TrafficType)
	}
}

func TestSplitWithRuleBasedSegment(t *testing.T) {
	file, err := ioutil.TempFile("", "localhost_test-*.json")
	if err != nil {
		t.Error("should not fail to open temp file. Got: ", err)
	}
	defer os.Remove(file.Name())

	if _, err := file.Write(jsonSplitWithRuleBasedSegment); err != nil {
		t.Error("writing to the file should not fail")
	}

	if err := file.Sync(); err != nil {
		t.Error("syncing the file should not fail")
	}

	logger := logging.NewLogger(nil)
	fetcher := NewFileSplitFetcher(file.Name(), logger, SplitFileFormatJSON)

	res, err := fetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(-1))
	if err != nil {
		t.Error("fetching should not fail")
		return
	}

	if len(res.FeatureFlags.Splits) != 1 {
		t.Error("should have 1 split, got:", len(res.FeatureFlags.Splits))
		return
	}

	split := res.FeatureFlags.Splits[0]
	if split.Name != "SPLIT_WITH_RULE" {
		t.Error("Expected split name SPLIT_WITH_RULE, got", split.Name)
	}

	if len(split.Conditions) != 2 {
		t.Error("Expected 2 conditions (rule based + default), got", len(split.Conditions))
		return
	}

	// First condition should be our rule based segment condition
	ruleBasedCondition := split.Conditions[0]
	if len(ruleBasedCondition.MatcherGroup.Matchers) != 1 {
		t.Error("Expected 1 matcher in first condition, got", len(ruleBasedCondition.MatcherGroup.Matchers))
		return
	}

	ruleBasedMatcher := ruleBasedCondition.MatcherGroup.Matchers[0]
	if ruleBasedMatcher.MatcherType != "IN_SEGMENT" {
		t.Error("Expected matcher type IN_SEGMENT in first condition, got", ruleBasedMatcher.MatcherType)
	}

	if ruleBasedMatcher.UserDefinedSegment == nil {
		t.Error("Expected non-nil UserDefinedSegment in first condition")
		return
	}

	if ruleBasedMatcher.UserDefinedSegment.SegmentName != "test-segment" {
		t.Error("Expected segment name test-segment in first condition, got", ruleBasedMatcher.UserDefinedSegment.SegmentName)
	}

	// Second condition should be the default ALL_KEYS condition
	defaultCondition := split.Conditions[1]
	if len(defaultCondition.MatcherGroup.Matchers) != 1 {
		t.Error("Expected 1 matcher in default condition, got", len(defaultCondition.MatcherGroup.Matchers))
		return
	}

	defaultMatcher := defaultCondition.MatcherGroup.Matchers[0]
	if defaultMatcher.MatcherType != "ALL_KEYS" {
		t.Error("Expected matcher type ALL_KEYS in default condition, got", defaultMatcher.MatcherType)
	}

	// Verify rule based segments
	if len(res.RuleBasedSegments.RuleBasedSegments) != 1 {
		t.Error("Expected 1 rule based segment, got", len(res.RuleBasedSegments.RuleBasedSegments))
		return
	}

	ruleBasedSegment := res.RuleBasedSegments.RuleBasedSegments[0]
	if ruleBasedSegment.Name != "test-segment" {
		t.Error("Expected rule based segment name test-segment, got", ruleBasedSegment.Name)
	}

	if ruleBasedSegment.TrafficTypeName != "user" {
		t.Error("Expected traffic type user, got", ruleBasedSegment.TrafficTypeName)
	}

	if ruleBasedSegment.Status != "ACTIVE" {
		t.Error("Expected status ACTIVE, got", ruleBasedSegment.Status)
	}

	if len(ruleBasedSegment.Conditions) != 1 {
		t.Error("Expected 1 condition in rule based segment, got", len(ruleBasedSegment.Conditions))
		return
	}

	rbsCondition := ruleBasedSegment.Conditions[0]
	if len(rbsCondition.MatcherGroup.Matchers) != 1 {
		t.Error("Expected 1 matcher in rule based segment condition, got", len(rbsCondition.MatcherGroup.Matchers))
		return
	}

	rbsMatcher := rbsCondition.MatcherGroup.Matchers[0]
	if rbsMatcher.MatcherType != "ALL_KEYS" {
		t.Error("Expected matcher type ALL_KEYS in rule based segment, got", rbsMatcher.MatcherType)
	}
}

func TestSplitWithOldTill(t *testing.T) {
	file, err := ioutil.TempFile("", "localhost_test-*.json")
	if err != nil {
		t.Error("should not fail to open temp file. Got: ", err)
	}
	defer os.Remove(file.Name())

	if _, err := file.Write(jsonSplitWithOldTill); err != nil {
		t.Error("writing to the file should not fail")
	}

	if err := file.Sync(); err != nil {
		t.Error("syncing the file should not fail")
	}

	logger := logging.NewLogger(nil)
	fetcher := NewFileSplitFetcher(file.Name(), logger, SplitFileFormatJSON)

	// Test with a change number higher than the till value
	_, err = fetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(100))
	if err == nil {
		t.Error("Expected error when till is less than change number, got nil")
	} else if err.Error() != "ignoring change, the till is less than storage change number" {
		t.Error("Unexpected error message:", err.Error())
	}

	// Test with a change number equal to the till value
	res, err := fetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(50))
	if err != nil {
		t.Error("Expected no error when till equals change number, got:", err)
	}
	if res.FeatureFlags.Till != 50 {
		t.Error("Expected till value 50, got:", res.FeatureFlags.Till)
	}

	// Test with a change number less than the till value
	res, err = fetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(25))
	if err != nil {
		t.Error("Expected no error when till is greater than change number, got:", err)
	}
	if res.FeatureFlags.Till != 25 {
		t.Error("Expected till value to match change number 25, got:", res.FeatureFlags.Till)
	}
}

func TestSplitWithOldRBSTill(t *testing.T) {
	file, err := ioutil.TempFile("", "localhost_test-*.json")
	if err != nil {
		t.Error("should not fail to open temp file. Got: ", err)
	}
	defer os.Remove(file.Name())

	if _, err := file.Write(jsonSplitWithOldRBSTill); err != nil {
		t.Error("writing to the file should not fail")
	}

	if err := file.Sync(); err != nil {
		t.Error("syncing the file should not fail")
	}

	logger := logging.NewLogger(nil)
	fetcher := NewFileSplitFetcher(file.Name(), logger, SplitFileFormatJSON)

	// Test with a change number higher than the RBS till value
	_, err = fetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(100))
	if err == nil {
		t.Error("Expected error when RBS till is less than change number, got nil")
	} else if err.Error() != "ignoring change, the till is less than storage change number" {
		t.Error("Unexpected error message:", err.Error())
	}

	// Test with a change number equal to the RBS till value
	res, err := fetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(50))
	if err != nil {
		t.Error("Expected no error when RBS till equals change number, got:", err)
	}
	if res.RuleBasedSegments.Till != 50 {
		t.Error("Expected RBS till value 50, got:", res.RuleBasedSegments.Till)
	}

	// Test with a change number less than the RBS till value
	res, err = fetcher.Fetch(service.MakeFlagRequestParams().WithChangeNumber(25))
	if err != nil {
		t.Error("Expected no error when RBS till is greater than change number, got:", err)
	}
	if res.RuleBasedSegments.Till != 50 {
		t.Error("Expected RBS till value to remain at 50, got:", res.RuleBasedSegments.Till)
	}
	if res.RuleBasedSegments.Since != 50 {
		t.Error("Expected RBS since value to be 50, got:", res.RuleBasedSegments.Since)
	}

	// Verify that the rule based segments are present
	if len(res.RuleBasedSegments.RuleBasedSegments) != 1 {
		t.Error("Expected 1 rule based segment, got", len(res.RuleBasedSegments.RuleBasedSegments))
	}
	ruleBasedSegment := res.RuleBasedSegments.RuleBasedSegments[0]
	if ruleBasedSegment.Name != "test-segment" {
		t.Error("Expected rule based segment name test-segment, got", ruleBasedSegment.Name)
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

	if res.FeatureFlags.Splits[0].Algo != 2 {
		t.Error("algo should be 2. Got: ", res.FeatureFlags.Splits[0].Algo)
	}

	if res.FeatureFlags.Splits[0].TrafficAllocation != 100 {
		t.Error("traffic allocation should be 100. Got: ", res.FeatureFlags.Splits[0].TrafficAllocation)
	}

	if res.FeatureFlags.Splits[0].TrafficAllocationSeed == 0 {
		t.Error("traffic allocation seed can't be 0")
	}

	if res.FeatureFlags.Splits[0].Seed == 0 {
		t.Error("seed can't be 0")
	}

	if res.FeatureFlags.Splits[0].DefaultTreatment != "control" {
		t.Error("default treatment should be control")
	}
}
