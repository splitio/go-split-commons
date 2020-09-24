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
	impressionTXT := `{"keyName":"some_key","treatment":"off","time":1234567890,"changeNumber":55555555,"label":"some label","bucketingKey":"some_bucket_key"}`
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

func TestPostImpressions(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	var expectedPT int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		sdkVersion := r.Header.Get("SplitSDKVersion")
		sdkMachine := r.Header.Get("SplitSDKMachineIP")

		if sdkVersion != fmt.Sprint("go-some") {
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
		if splitSDKImpressionsMode != conf.Debug {
			t.Error("SDK Impressions Mode HEADER not match", splitSDKImpressionsMode)
		}

		rBody, _ := ioutil.ReadAll(r.Body)
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
	err2 := impressionRecorder.Record(impressions, dtos.Metadata{SDKVersion: "go-some", MachineIP: "127.0.0.1", MachineName: "SOME_MACHINE_NAME"}, map[string]string{"SplitSDKImpressionsMode": conf.Debug})
	if err2 != nil {
		t.Error(err2)
	}
}

func TestPostMetricsLatency(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		sdkVersion := r.Header.Get("SplitSDKVersion")
		sdkMachine := r.Header.Get("SplitSDKMachineIP")

		if sdkVersion != fmt.Sprint("go-some") {
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
		var latenciesInPost []dtos.LatenciesDTO
		err := json.Unmarshal(rBody, &latenciesInPost)
		if err != nil {
			t.Error(err)
			return
		}

		if latenciesInPost[0].MetricName != "some_metric_name" ||
			latenciesInPost[0].Latencies[5] != 1234567890 {
			t.Error("Latencies arrived mal-formed")
		}

		fmt.Fprintln(w, "ok")
	}))
	defer ts.Close()

	var latencyValues = make([]int64, 23) //23 maximun number of buckets
	latencyValues[5] = 1234567890
	var latencies []dtos.LatenciesDTO
	latencies = append(latencies, dtos.LatenciesDTO{MetricName: "some_metric_name", Latencies: latencyValues})

	metricsRecorder := NewHTTPMetricsRecorder(
		"",
		conf.AdvancedConfig{
			EventsURL: ts.URL,
			SdkURL:    ts.URL,
		},
		logger,
	)
	err2 := metricsRecorder.RecordLatencies(latencies, dtos.Metadata{SDKVersion: "go-some", MachineIP: "127.0.0.1", MachineName: "ip-127-0-0-1"})
	if err2 != nil {
		t.Error(err2)
	}
}

func TestPostMetricsCounters(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		sdkVersion := r.Header.Get("SplitSDKVersion")
		sdkMachine := r.Header.Get("SplitSDKMachineIP")

		if sdkVersion != fmt.Sprint("go-some") {
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
		var countersInPost []dtos.CounterDTO
		err := json.Unmarshal(rBody, &countersInPost)
		if err != nil {
			t.Error(err)
			return
		}

		if countersInPost[0].MetricName != "counter_1" ||
			countersInPost[0].Count != 111 ||
			countersInPost[1].MetricName != "counter_2" ||
			countersInPost[1].Count != 222 {
			t.Error("Counters arrived mal-formed")
		}

		fmt.Fprintln(w, "ok")
	}))
	defer ts.Close()

	var counters []dtos.CounterDTO
	counters = append(
		counters,
		dtos.CounterDTO{MetricName: "counter_1", Count: 111},
		dtos.CounterDTO{MetricName: "counter_2", Count: 222},
	)

	metricsRecorder := NewHTTPMetricsRecorder(
		"",
		conf.AdvancedConfig{
			EventsURL: ts.URL,
			SdkURL:    ts.URL,
		},
		logger,
	)

	err2 := metricsRecorder.RecordCounters(counters, dtos.Metadata{SDKVersion: "go-some", MachineIP: "127.0.0.1", MachineName: "ip-127-0-0-1"})
	if err2 != nil {
		t.Error(err2)
	}
}

func TestPostMetricsGauge(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		sdkVersion := r.Header.Get("SplitSDKVersion")
		sdkMachine := r.Header.Get("SplitSDKMachineIP")

		if sdkVersion != fmt.Sprint("go-some") {
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
		var gaugesInPost dtos.GaugeDTO
		err := json.Unmarshal(rBody, &gaugesInPost)
		if err != nil {
			t.Error(err)
			return
		}

		if gaugesInPost.MetricName != "gauge_1" ||
			gaugesInPost.Gauge != 111.1 {
			t.Error("Gauges arrived mal-formed")
		}

		fmt.Fprintln(w, "ok")
	}))
	defer ts.Close()

	var gauge dtos.GaugeDTO
	gauge = dtos.GaugeDTO{MetricName: "gauge_1", Gauge: 111.1}

	metricsRecorder := NewHTTPMetricsRecorder(
		"",
		conf.AdvancedConfig{
			EventsURL: ts.URL,
			SdkURL:    ts.URL,
		},
		logger,
	)

	err2 := metricsRecorder.RecordGauge(gauge, dtos.Metadata{SDKVersion: "go-some", MachineIP: "127.0.0.1", MachineName: "ip-127-0-0-1"})
	if err2 != nil {
		t.Error(err2)
	}

}
