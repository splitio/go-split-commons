package api

import (
	"testing"

	"github.com/splitio/go-split-commons/dtos"
)

func TestAddMetadataToHeaders(t *testing.T) {
	headers := AddMetadataToHeaders(dtos.Metadata{SDKVersion: "go-some", MachineIP: na, MachineName: unknown}, map[string]string{"some": "some"}, nil)
	if headers[splitSDKVersion] != "go-some" {
		t.Error("Wrong SDK Version")
	}
	if _, ok := headers[splitSDKMachineIP]; ok {
		t.Error("Should not parse MachineIP")
	}
	if _, ok := headers[splitSDKMachineName]; ok {
		t.Error("Should not parse MachineName")
	}
	if headers["some"] != "some" {
		t.Error("Wrong extra header")
	}
	if _, ok := headers[splitSDKClientKey]; ok {
		t.Error("Should not parse clientKey")
	}

	myKey := "test"
	headers2 := AddMetadataToHeaders(dtos.Metadata{SDKVersion: "go-some", MachineIP: "1.1.1.1", MachineName: "name"}, map[string]string{"some": "some"}, &myKey)
	if headers2[splitSDKVersion] != "go-some" {
		t.Error("Wrong SDK Version")
	}
	if headers2[splitSDKMachineIP] != "1.1.1.1" {
		t.Error("Wrong MachineIP")
	}
	if headers2[splitSDKMachineName] != "name" {
		t.Error("Wrong MachineName")
	}
	if headers2["some"] != "some" {
		t.Error("Wrong extra header")
	}
}
