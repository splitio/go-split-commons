package api

import (
	"encoding/json"

	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-toolkit/logging"
)

const prodAuthURL = "https://auth.split-stage.io"

func getAuthURL(cfg *conf.AdvancedConfig) string {
	if cfg != nil && cfg.AuthURL != "" {
		return cfg.AuthURL
	}
	return prodAuthURL
}

// AuthAPIClient struct is responsible for authenticating client for push services
type AuthAPIClient struct {
	client Client
	logger logging.LoggerInterface
}

// NewAuthAPIClient instantiates and return an AuthAPIClient
func NewAuthAPIClient(
	apikey string,
	cfg *conf.AdvancedConfig,
	logger logging.LoggerInterface,
) *AuthAPIClient {
	return &AuthAPIClient{
		client: NewHTTPClient(apikey, cfg, getAuthURL(cfg), logger),
		logger: logger,
	}
}

// Authenticate performs authentication for push services
func (a *AuthAPIClient) Authenticate() (*dtos.Token, error) {
	raw, err := a.client.Get("/api/auth")
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
