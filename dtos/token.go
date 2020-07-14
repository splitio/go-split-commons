package dtos

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

const gracePeriod = 10 * time.Minute

// Token dto
type Token struct {
	Token       string `json:"token"`
	PushEnabled bool   `json:"pushEnabled"`
}

// TokenPayload payload dto
type TokenPayload struct {
	Capabilitites string `json:"x-ably-capability"`
	Exp           int64  `json:"exp"`
	Iat           int64  `json:"iat"`
}

// ParsedCapabilities capabilities
type ParsedCapabilities map[string][]string

// ChannelList grabs the channel list from capabilities
func (t *Token) ChannelList() ([]string, error) {
	if !t.PushEnabled || t.Token == "" {
		return nil, errors.New("Push disabled or no token set")
	}

	tokenParts := strings.Split(t.Token, ".")
	if len(tokenParts) < 2 {
		return nil, errors.New("Cannot decode token")
	}
	decodedPayload, err := base64.RawURLEncoding.DecodeString(tokenParts[1])
	if err != nil {
		return nil, err
	}

	var parsedPayload TokenPayload
	err = json.Unmarshal(decodedPayload, &parsedPayload)
	if err != nil {
		return nil, err
	}

	var parsedCapabilities ParsedCapabilities
	err = json.Unmarshal([]byte(parsedPayload.Capabilitites), &parsedCapabilities)
	if err != nil {
		return nil, err
	}

	channelList := make([]string, 0, len(parsedCapabilities))
	for channelName := range parsedCapabilities {
		channelList = append(channelList, channelName)
	}

	return channelList, nil
}

// CalculateNextTokenExpiration calculates next token expiration
func (t *Token) CalculateNextTokenExpiration() (time.Duration, error) {
	if !t.PushEnabled || t.Token == "" {
		return 0, errors.New("Push disabled or no token set")
	}

	tokenParts := strings.Split(t.Token, ".")
	if len(tokenParts) < 2 {
		return 0, errors.New("Cannot decode token")
	}
	decodedPayload, err := base64.RawURLEncoding.DecodeString(tokenParts[1])
	if err != nil {
		return 0, err
	}

	var parsedPayload TokenPayload
	err = json.Unmarshal(decodedPayload, &parsedPayload)
	if err != nil {
		return 0, err
	}

	tokenDuration := parsedPayload.Exp - parsedPayload.Iat
	return time.Duration(tokenDuration)*time.Second - gracePeriod, nil
}
