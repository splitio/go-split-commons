package api

import (
	"encoding/json"

	"github.com/splitio/go-split-commons/v5/conf"
	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-split-commons/v5/service"
	"github.com/splitio/go-split-commons/v5/spec"
	"github.com/splitio/go-toolkit/v5/common"
	"github.com/splitio/go-toolkit/v5/logging"
)

// AuthAPIClient struct is responsible for authenticating client for push services
type AuthAPIClient struct {
	client Client
	logger logging.LoggerInterface
}

// NewAuthAPIClient instantiates and return an AuthAPIClient
func NewAuthAPIClient(apikey string, cfg conf.AdvancedConfig, logger logging.LoggerInterface, metadata dtos.Metadata) *AuthAPIClient {
	return &AuthAPIClient{
		client: NewHTTPClient(apikey, cfg, cfg.AuthServiceURL, logger, metadata),
		logger: logger,
	}
}

// Authenticate performs authentication for push services
func (a *AuthAPIClient) Authenticate() (*dtos.Token, error) {
	raw, err := a.client.Get("/api/v2/auth", service.MakeAuthFetchOptions(common.StringRef(spec.FlagSpec)))
	if err != nil {
		a.logger.Error("Error while authenticating for streaming", err)
		return nil, err
	}

	token := dtos.Token{}
	err = json.Unmarshal(raw, &token)
	if err != nil {
		return nil, err
	}
	return &token, nil
}
