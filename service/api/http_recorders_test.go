// Package api contains all functions and dtos Split APIs
package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-toolkit/logging"
)

func TestImpressionRecord(t *testing.T) {
	impressionTXT := `{"k":"some_key","t":"off","m":1234567890,"c":55555555,"r":"some label","b":"some_bucket_key"}`
	impressionRecord := &dtos.ImpressionDTO{
		KeyName:      "some_key",
		Treatment:    "off",
		Time:         1234567890,
		ChangeNumber: 55555555,
		Label:        "some label",
		BucketingKey: "some_bucket_key"}

	marshalImpression, err := json.Marshal(impressionRecord)
	if err != nil {
		t.Error(err)
	}

	if string(marshalImpression) != impressionTXT {
		t.Error("Error marshaling impression")
	}
}

func TestImpressionRecordBulk(t *testing.T) {
	impressionTXT := `{"f":"some_feature","i":[{"k":"some_key","t":"off","m":1234567890,"c":55555555,"r":"some label","b":"some_bucket_key"}]}`
	impressionRecords := &dtos.ImpressionsDTO{
		TestName: "some_feature",
		KeyImpressions: []dtos.ImpressionDTO{{
			KeyName:      "some_key",
			Treatment:    "off",
			Time:         1234567890,
			ChangeNumber: 55555555,
			Label:        "some label",
			BucketingKey: "some_bucket_key"}},
	}

	marshalImpression, err := json.Marshal(impressionRecords)
	if err != nil {
		t.Error(err)
	}

	if string(marshalImpression) != impressionTXT {
		t.Error("Error marshaling impression", string(marshalImpression), impressionTXT)
	}
}

func TestPostImpressions(t *testing.T) {
	impressionsTXT := `[{"f":"some_test_2","i":[{"k":"some_key_1","t":"on","m":1234567890,"c":9876543210,"r":"some_label_1","b":"some_bucket_key_1"}]},{"f":"some_test","i":[{"k":"some_key_2","t":"off","m":1234567890,"c":9876543210,"r":"some_label_2","b":"some_bucket_key_2"}]}]`
	logger := logging.NewLogger(&logging.LoggerOptions{})

	var expectedPT int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		sdkVersion := r.Header.Get("SplitSDKVersion")
		sdkMachine := r.Header.Get("SplitSDKMachineIP")

		if sdkVersion != "go-some" {
			t.Error("SDK Version HEADER not match")
			t.Error(sdkVersion)
		}

		if sdkMachine != "127.0.0.1" {
			t.Error("SDK Machine HEADER not match")
		}

		sdkMachineName := r.Header.Get("SplitSDKMachineName")
		if sdkMachineName != "SOME_MACHINE_NAME" {
			t.Error("SDK Machine Name HEADER not match", sdkMachineName)
		}

		splitSDKImpressionsMode := r.Header.Get("SplitSDKImpressionsMode")
		if splitSDKImpressionsMode != conf.ImpressionsModeDebug {
			t.Error("SDK Impressions Mode HEADER not match", splitSDKImpressionsMode)
		}

		rBody, _ := ioutil.ReadAll(r.Body)
		if string(rBody) != impressionsTXT {
			t.Error("Wrong payload")
		}
		var impressionsInPost []dtos.ImpressionsDTO
		err := json.Unmarshal(rBody, &impressionsInPost)
		if err != nil {
			t.Error(err)
			return
		}

		if len(impressionsInPost) != 2 {
			t.Error("Posted impressions arrived mal-formed")
		}

		for _, impressions := range impressionsInPost {
			switch impressions.TestName {
			case "some_test_2":
				if impressions.KeyImpressions[0].KeyName != "some_key_1" {
					t.Error("Wrong impression")
				}
				for _, ki := range impressions.KeyImpressions {
					if ki.Pt != expectedPT {
						t.Error("incorrect pt")
					}
				}
			case "some_test":
				if impressions.KeyImpressions[0].KeyName != "some_key_2" {
					t.Error("Wrong impression")
				}
				for _, ki := range impressions.KeyImpressions {
					if ki.Pt != expectedPT {
						t.Error("incorrect pt")
					}
				}
			default:
				t.Error("Unexpected Impression")
			}
		}

		fmt.Fprintln(w, "ok")
	}))
	defer ts.Close()

	imp1 := dtos.ImpressionsDTO{
		TestName: "some_test_2",
		KeyImpressions: []dtos.ImpressionDTO{{
			KeyName:      "some_key_1",
			Treatment:    "on",
			Time:         1234567890,
			ChangeNumber: 9876543210,
			Label:        "some_label_1",
			BucketingKey: "some_bucket_key_1",
		}},
	}
	imp2 := dtos.ImpressionsDTO{
		TestName: "some_test",
		KeyImpressions: []dtos.ImpressionDTO{{
			KeyName:      "some_key_2",
			Treatment:    "off",
			Time:         1234567890,
			ChangeNumber: 9876543210,
			Label:        "some_label_2",
			BucketingKey: "some_bucket_key_2",
		}},
	}

	impressions := make([]dtos.ImpressionsDTO, 0)
	impressions = append(impressions, imp1, imp2)

	impressionRecorder := NewHTTPImpressionRecorder(
		"",
		conf.AdvancedConfig{
			EventsURL: ts.URL,
			SdkURL:    ts.URL,
		},
		logger,
	)

	expectedPT = 0
	err2 := impressionRecorder.Record(impressions, dtos.Metadata{SDKVersion: "go-some", MachineIP: "127.0.0.1", MachineName: "SOME_MACHINE_NAME"}, map[string]string{"SplitSDKImpressionsMode": conf.ImpressionsModeDebug})
	if err2 != nil {
		t.Error(err2)
	}
}

func TestPostTelemetryStats(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		sdkVersion := r.Header.Get("SplitSDKVersion")
		sdkMachine := r.Header.Get("SplitSDKMachineIP")

		if sdkVersion != "go-some" {
			t.Error("SDK Version HEADER not match")
		}

		if sdkMachine != "127.0.0.1" {
			t.Error("SDK Machine HEADER not match")
		}

		sdkMachineName := r.Header.Get("SplitSDKMachineName")
		if sdkMachineName != "ip-127-0-0-1" {
			t.Error("SDK Machine Name HEADER not match", sdkMachineName)
		}

		rBody, _ := ioutil.ReadAll(r.Body)
		var statsInPost dtos.Stats
		err := json.Unmarshal(rBody, &statsInPost)
		if err != nil {
			t.Error(err)
			return
		}

		if statsInPost.TokenRefreshes != 10 {
			t.Error("Unexpected value")
		}
		if statsInPost.LastSynchronizations.Splits != 123456789 {
			t.Error("Unexpected value")
		}

		fmt.Fprintln(w, "ok")
	}))
	defer ts.Close()

	telemetryRecorder := NewHTTPTelemetryRecorder(
		"",
		conf.AdvancedConfig{
			TelemetryServiceURL: ts.URL,
		},
		logger,
	)

	err := telemetryRecorder.RecordStats(dtos.Stats{
		LastSynchronizations: &dtos.LastSynchronization{
			Splits: 123456789,
		},
		TokenRefreshes: 10,
	}, dtos.Metadata{SDKVersion: "go-some", MachineIP: "127.0.0.1", MachineName: "ip-127-0-0-1"})
	if err != nil {
		t.Error(err)
	}
}
