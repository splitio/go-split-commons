package api

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/splitio/go-split-commons/v4/conf"
	"github.com/splitio/go-split-commons/v4/dtos"
	"github.com/splitio/go-split-commons/v4/service/api/mocks"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestAuthErr(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	mockedAuth := AuthAPIClient{
		client: mocks.ClientMock{
			GetCall: func(service string, headers map[string]string) ([]byte, error) {
				if service != "/api/auth" {
					t.Error("Wrong service passed")
				}
				return nil, errors.New("Some")
			},
		},
		logger: logger,
	}

	token, err := mockedAuth.Authenticate()
	if token != nil {
		t.Error("It should return nil")
	}
	if err == nil {
		t.Error("It should return some err")
	}
}

func TestAuthPushEnabledFalse(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	mockedAuth := AuthAPIClient{
		client: mocks.ClientMock{
			GetCall: func(service string, headers map[string]string) ([]byte, error) {
				if service != "/api/auth" {
					t.Error("Wrong service passed")
				}
				return []byte("{\"pushEnabled\":false,\"token\":\"\"}"), nil
			},
		},
		logger: logger,
	}

	token, err := mockedAuth.Authenticate()
	if err != nil {
		t.Error("It should not return err")
	}
	if token == nil {
		t.Error("It should not return nil")
	}

	if token.PushEnabled {
		t.Error("It should be false")
	}
	if len(token.Token) != 0 {
		t.Error("It should be empty")
	}
}

func TestAuthPushEnabledTrue(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	mockedAuth := AuthAPIClient{
		client: mocks.ClientMock{
			GetCall: func(service string, headers map[string]string) ([]byte, error) {
				if service != "/api/auth" {
					t.Error("Wrong service passed")
				}
				return []byte("{\"pushEnabled\":true,\"token\":\"eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE1OTE3NDQzOTksImlhdCI6MTU5MTc0MDc5OX0.EcWYtI0rlA7LCVJ5tYldX-vpfMRIc_1HT68-jhXseCo\"}"), nil
			},
		},
		logger: logger,
	}

	token, err := mockedAuth.Authenticate()
	if err != nil {
		t.Error("It should not return err")
	}
	if token == nil {
		t.Error("It should not return nil")
	}

	if !token.PushEnabled {
		t.Error("It should be true")
	}
	if len(token.Token) == 0 {
		t.Error("It should not be empty")
	}
}

func TestAuthLogic(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, http.StatusText(http.StatusInternalServerError),
			http.StatusInternalServerError)
	}))
	defer ts.Close()

	authClient := NewAuthAPIClient(
		"",
		conf.AdvancedConfig{},
		logger,
		dtos.Metadata{},
	)

	token, err := authClient.Authenticate()
	if err == nil {
		t.Error("Error expected but not found")
	}
	if token != nil {
		t.Error("It should be nil")
	}
}
