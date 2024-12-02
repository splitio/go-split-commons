// Package api contains all functions and dtos Split APIs
package dtos

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

var splitsMock, _ = os.ReadFile("../testdata/splits_mock.json")
var splitMock, _ = os.ReadFile("../testdata/split_mock.json")

func TestSplitDTO(t *testing.T) {
	mockedData := fmt.Sprintf(string(splitsMock), splitMock)

	var splitChangesDtoFromMock SplitChangesDTO
	var splitChangesDtoFromMarshal SplitChangesDTO

	err := json.Unmarshal([]byte(mockedData), &splitChangesDtoFromMock)
	if err != nil {
		t.Error("Error parsing split changes JSON ", err)
	}

	if dataSerialize, err := splitChangesDtoFromMock.Splits[0].MarshalBinary(); err != nil {
		t.Error(err)
	} else {
		marshalData := fmt.Sprintf(string(splitsMock), dataSerialize)
		err2 := json.Unmarshal([]byte(marshalData), &splitChangesDtoFromMarshal)
		if err2 != nil {
			t.Error("Error parsing split changes JSON ", err)
		}

		if splitChangesDtoFromMarshal.Splits[0].ChangeNumber !=
			splitChangesDtoFromMock.Splits[0].ChangeNumber {
			t.Error("Marshal struct mal formed [ChangeNumber]")
		}

		if splitChangesDtoFromMarshal.Splits[0].Name !=
			splitChangesDtoFromMock.Splits[0].Name {
			t.Error("Marshal struct mal formed [Name]")
		}

		if splitChangesDtoFromMarshal.Splits[0].Killed !=
			splitChangesDtoFromMock.Splits[0].Killed {
			t.Error("Marshal struct mal formed [Killed]")
		}

		if splitChangesDtoFromMarshal.Splits[0].Configurations == nil {
			t.Error("Marshal struct mal formed [Configurations]")
		}

		if splitChangesDtoFromMarshal.Splits[0].Configurations["of"] !=
			splitChangesDtoFromMock.Splits[0].Configurations["of"] {
			t.Error("Marshal struct mal formed [Configurations]")
		}

		if splitChangesDtoFromMarshal.Splits[0].Configurations["on"] !=
			splitChangesDtoFromMock.Splits[0].Configurations["on"] {
			t.Error("Marshal struct mal formed [Configurations]")
		}

		if len(splitChangesDtoFromMarshal.Splits[0].Sets) != 0 {
			t.Error("Marshal struct mal formed [Sets]")
		}

		condition := splitChangesDtoFromMarshal.Splits[0].Conditions[0]
		if condition.ConditionType != "WHITELIST" {
			t.Error("Marshal struct mal formed [ConditionType]. Actual: ", condition.ConditionType)
		}

		matcher := condition.MatcherGroup.Matchers[0]
		if matcher.MatcherType != "IN_LARGE_SEGMENT" {
			t.Error("Marshal struct mal formed [MatcherType]")
		}

		if matcher.UserDefinedLargeSegment.LargeSegmentName != "mauro_sanz_ls" {
			t.Error("Marshal struct mal formed [UserDefinedLargeSegment]")
		}
	}
}
