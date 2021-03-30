package api

import "github.com/splitio/go-split-commons/v3/dtos"

const (
	splitSDKVersion     = "SplitSDKVersion"
	splitSDKMachineName = "SplitSDKMachineName"
	splitSDKMachineIP   = "SplitSDKMachineIP"

	unknown = "unknown"
	na      = "NA"
)

func ParseHeaderMetadata(metadata dtos.Metadata, extraHeaders map[string]string) map[string]string {
	headers := make(map[string]string)
	headers[splitSDKVersion] = metadata.SDKVersion
	if metadata.MachineName != na && metadata.MachineName != unknown {
		headers[splitSDKMachineName] = metadata.MachineName
	}
	if metadata.MachineIP != na && metadata.MachineIP != unknown {
		headers[splitSDKMachineIP] = metadata.MachineIP
	}
	for header, value := range extraHeaders {
		headers[header] = value
	}
	return headers
}
