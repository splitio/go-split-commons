package dtos

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/splitio/go-toolkit/v5/datastructures/set"
)

func TestTokenUnmarshalJSON(t *testing.T) {
	// Legacy shape: top-level pushEnabled wins, config.streaming.enabled is ignored if present.
	var legacy Token
	if err := json.Unmarshal([]byte(`{"token":"abc","pushEnabled":true,"config":{"streaming":{"enabled":false}}}`), &legacy); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !legacy.PushEnabled {
		t.Error("expected top-level pushEnabled to take precedence")
	}

	var legacyFalse Token
	if err := json.Unmarshal([]byte(`{"token":"abc","pushEnabled":false}`), &legacyFalse); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if legacyFalse.PushEnabled {
		t.Error("expected pushEnabled=false to be honored")
	}

	// New shape: no top-level pushEnabled, fall back to config.streaming.enabled.
	var viaConfig Token
	if err := json.Unmarshal([]byte(`{"token":"abc","config":{"streaming":{"enabled":true}}}`), &viaConfig); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !viaConfig.PushEnabled {
		t.Error("expected config.streaming.enabled fallback to enable push")
	}
	if viaConfig.Token != "abc" {
		t.Error("expected token field to still be parsed")
	}

	// Neither field present: defaults to false, same as the zero value.
	var neither Token
	if err := json.Unmarshal([]byte(`{"token":"abc"}`), &neither); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if neither.PushEnabled {
		t.Error("expected PushEnabled to default to false")
	}
}

func TestTokenChannels(t *testing.T) {
	token := Token{
		PushEnabled: false,
		Token:       "eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE1OTE3NDQzOTksImlhdCI6MTU5MTc0MDc5OX0.EcWYtI0rlA7LCVJ5tYldX-vpfMRIc_1HT68-jhXseCo",
	}

	result, err := token.ChannelList()
	if result != nil {
		t.Error("It should be nil")
	}
	if err == nil {
		t.Error("It should not be nil")
	}

	token2 := Token{
		PushEnabled: true,
		Token:       "",
	}
	result, err = token2.ChannelList()
	if result != nil {
		t.Error("It should be nil")
	}
	if err == nil {
		t.Error("It should not be nil")
	}

	token3 := Token{
		PushEnabled: true,
		Token:       "eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE1OTE3NDQzOTksImlhdCI6MTU5MTc0MDc5OX0.EcWYtI0rlA7LCVJ5tYldX-vpfMRIc_1HT68-jhXseCo",
	}
	result, err = token3.ChannelList()
	if err != nil {
		t.Error("It should be nil")
	}
	channels := set.NewThreadSafeSet()
	for _, r := range result {
		channels.Add(r)
	}
	if result == nil || len(result) != 4 {
		t.Error("It should not be nil")
	}
	if !channels.Has("NzM2MDI5Mzc0_MTgyNTg1MTgwNg==_segments") {
		t.Error("It should exist")
	}
	if !channels.Has("NzM2MDI5Mzc0_MTgyNTg1MTgwNg==_splits") {
		t.Error("It should exist")
	}
	if !channels.Has("[?occupancy=metrics.publishers]control_pri") {
		t.Error("It should exist")
	}
	if !channels.Has("[?occupancy=metrics.publishers]control_sec") {
		t.Error("It should exist")
	}
}

func TestTokenRefresh(t *testing.T) {
	token := Token{
		PushEnabled: false,
		Token:       "eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE1OTE3NDQzOTksImlhdCI6MTU5MTc0MDc5OX0.EcWYtI0rlA7LCVJ5tYldX-vpfMRIc_1HT68-jhXseCo",
	}

	_, err := token.CalculateNextTokenExpiration()
	if err == nil {
		t.Error("It should not be nil")
	}

	token2 := Token{
		PushEnabled: true,
		Token:       "",
	}
	_, err = token2.CalculateNextTokenExpiration()
	if err == nil {
		t.Error("It should not be nil")
	}

	token3 := Token{
		PushEnabled: true,
		Token:       "eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE1OTE3NDQzOTksImlhdCI6MTU5MTc0MDc5OX0.EcWYtI0rlA7LCVJ5tYldX-vpfMRIc_1HT68-jhXseCo",
	}
	result, err := token3.CalculateNextTokenExpiration()
	if err != nil {
		t.Error("It should be nil")
	}
	if result != 50*time.Minute {
		t.Error("It should be 50m")
	}
}
