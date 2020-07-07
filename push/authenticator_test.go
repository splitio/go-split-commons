package push

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service/mocks"
	"github.com/splitio/go-toolkit/logging"
)

func TestAuthenticator(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	status := make(chan interface{}, 1)

	authClient := mocks.MockAuthClient{
		AuthenticateCall: func() (*dtos.Token, error) {
			return &dtos.Token{
				Token:       "eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE1OTE3NDQzOTksImlhdCI6MTU5MTc0MDc5OX0.EcWYtI0rlA7LCVJ5tYldX-vpfMRIc_1HT68-jhXseCo",
				PushEnabled: true,
			}, nil
		},
	}

	authenticator := NewAuthenticator(status, authClient, logger)
	authenticator.Start()

	var token *dtos.Token
	st := <-status
	switch v := st.(type) {
	case *dtos.Token:
		token = v
	case int:
		t.Error("It should send return int")
	default:
		t.Error("It should send return int")
	}

	if authenticator.backoffAuthentication.IsRunning() {
		t.Error("It should stop")
	}
	if !token.PushEnabled {
		t.Error("Unexpected result")
	}
}

func TestAuthenticatorError(t *testing.T) {
	logger := logging.NewLogger(&logging.LoggerOptions{})

	status := make(chan interface{}, 1)

	authClient := mocks.MockAuthClient{
		AuthenticateCall: func() (*dtos.Token, error) {
			return nil, errors.New("some")
		},
	}

	authenticator := NewAuthenticator(status, authClient, logger)
	authenticator.Start()

	st := <-status
	switch v := st.(type) {
	case *dtos.Token:
		t.Error("It should send return int")
	case int:
		if v != Finished {
			t.Error("Wrong message received")
		}
	default:
		t.Error("Wrong message received")
	}

	time.Sleep(100 * time.Millisecond)
	if authenticator.backoffAuthentication.IsRunning() {
		t.Error("It should stop")
	}
}

func TestAuthenticatorWithRetries(t *testing.T) {
	attempts := 0
	logger := logging.NewLogger(&logging.LoggerOptions{})

	status := make(chan interface{}, 1)

	authClient := mocks.MockAuthClient{
		AuthenticateCall: func() (*dtos.Token, error) {
			if attempts == 2 {
				return &dtos.Token{
					Token:       "eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE1OTE3NDQzOTksImlhdCI6MTU5MTc0MDc5OX0.EcWYtI0rlA7LCVJ5tYldX-vpfMRIc_1HT68-jhXseCo",
					PushEnabled: true,
				}, nil
			}
			attempts++
			return nil, dtos.HTTPError{
				Code:    http.StatusInternalServerError,
				Message: "Error",
			}
		},
	}

	authenticator := NewAuthenticator(status, authClient, logger)
	authenticator.Start()

	var token *dtos.Token
	st := <-status
	switch v := st.(type) {
	case int:
		if v != Retrying {
			t.Error("It should retry")
		}
	default:
		t.Error("It should send return int")
	}
	st2 := <-status
	switch v := st2.(type) {
	case int:
		if v != Retrying {
			t.Error("It should retry")
		}
	default:
		t.Error("It should send return int")
	}
	st3 := <-status
	switch v := st3.(type) {
	case *dtos.Token:
		token = v
	case int:
		t.Error("It should send return int")
	default:
		t.Error("It should send return int")
	}

	time.Sleep(100 * time.Millisecond)
	if authenticator.backoffAuthentication.IsRunning() {
		t.Error("It should stop")
	}
	if !token.PushEnabled {
		t.Error("Unexpected result")
	}
}
