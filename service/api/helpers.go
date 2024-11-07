package api

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"

	"github.com/splitio/go-split-commons/v6/dtos"
	"github.com/splitio/go-split-commons/v6/service/api/specs"
)

const (
	splitSDKVersion     = "SplitSDKVersion"
	splitSDKMachineName = "SplitSDKMachineName"
	splitSDKMachineIP   = "SplitSDKMachineIP"
	splitSDKClientKey   = "SplitSDKClientKey"

	unknown = "unknown"
	na      = "NA"
)

const (
	// Unknown format
	Unknown = iota
	// Csv format
	Csv
)

// AddMetadataToHeaders adds metadata in headers
func AddMetadataToHeaders(metadata dtos.Metadata, extraHeaders map[string]string, clientKey *string) map[string]string {
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
	if clientKey != nil {
		headers[splitSDKClientKey] = *clientKey
	}
	return headers
}

func csvReader(response *http.Response, rfd dtos.RfdDTO) (*dtos.LargeSegment, error) {
	switch rfd.Version {
	case specs.MEMBERSHIP_V10:
		keys := make([]string, 0, rfd.TotalKeys)
		reader := csv.NewReader(response.Body)
		for {
			record, err := reader.Read()
			if err != nil {
				if err == io.EOF {
					break
				}

				return nil, fmt.Errorf("error reading csv file. %w", err)
			}

			if l := len(record); l != 1 {
				return nil, fmt.Errorf("unssuported file content. The file has multiple columns")
			}

			keys = append(keys, record[0])
		}

		return &dtos.LargeSegment{
			Name:         rfd.Name,
			Keys:         keys,
			ChangeNumber: rfd.ChangeNumber,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported csv version %s", rfd.Version)
	}
}
