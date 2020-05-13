package conf

import "crypto/tls"

// RedisConfig struct is used to cofigure the redis parameters
type RedisConfig struct {
	Host      string
	Port      int
	Database  int
	Password  string
	Prefix    string
	TLSConfig *tls.Config
}
