package push

import (
	"errors"
	"net/http"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-split-commons/service"
	"github.com/splitio/go-toolkit/backoff"
	"github.com/splitio/go-toolkit/logging"
)

const (
	// Retrying backoff is running
	Retrying = iota
	// Finished backoff ended
	Finished
)

const (
	maxSecondsRetry = 1800
)

// Authenticator struct
type Authenticator struct {
	backoffAuthentication *backoff.BackOff
	logger                logging.LoggerInterface
}

// NewAuthenticator creates new authenticator
func NewAuthenticator(authentication chan interface{}, authClient service.AuthClient, logger logging.LoggerInterface) *Authenticator {
	perform := func(logger logging.LoggerInterface) (bool, error) {
		token, err := authClient.Authenticate()
		if err != nil {
			errType, ok := err.(dtos.HTTPError)
			if ok && errType.Code >= http.StatusInternalServerError {
				logger.Info("Continue retrying")
				authentication <- Retrying
				return true, nil // It will continue retrying
			}
			authentication <- Finished
			return false, errors.New("Error authenticating")
		}
		authentication <- token
		return false, nil // Result is OK, Stopping Here, no more backoff
	}

	backoffAuthentication := backoff.NewBackOff("PerformAuthentication", perform, 1, float64(maxSecondsRetry), logger)
	if backoffAuthentication == nil {
		return nil
	}
	return &Authenticator{
		backoffAuthentication: backoffAuthentication,
		logger:                logger,
	}
}

// Start starts backkoff authentication
func (a *Authenticator) Start() {
	if !a.backoffAuthentication.IsRunning() {
		a.backoffAuthentication.Start()
	}
}

// Stop stops backoff authentication
func (a *Authenticator) Stop() {
	if a.backoffAuthentication.IsRunning() {
		a.backoffAuthentication.Stop(false)
	}
}
