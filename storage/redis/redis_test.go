package redis

import (
	"errors"
	"net"
	"testing"

	"github.com/splitio/go-split-commons/v8/conf"
	"github.com/splitio/go-toolkit/v5/logging"
)

func TestRedisConnection(t *testing.T) {

	client, err := NewRedisClient(&conf.RedisConfig{Host: "localhost", Port: 7658}, logging.NewLogger(nil))

	if client != nil {
		t.Error("client should be nil")
	}

	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Errorf("expected a network error. Got: %T", err)
	}
}
