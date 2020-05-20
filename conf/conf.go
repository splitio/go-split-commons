package conf

// RedisConfig struct is used to cofigure the redis parameters
type RedisConfig struct {
	Host     string `json:"host" split-default-value:"localhost" split-cli-option:"redis-host" split-cli-description:"Redis server hostname"`
	Port     int    `json:"port" split-default-value:"6379" split-cli-option:"redis-port" split-cli-description:"Redis Server port"`
	Database int    `json:"db" split-default-value:"0" split-cli-option:"redis-db" split-cli-description:"Redis DB"`
	Password string `json:"password" split-default-value:"" split-cli-option:"redis-pass" split-cli-description:"Redis password"`
	Prefix   string `json:"prefix" split-default-value:"" split-cli-option:"redis-prefix" split-cli-description:"Redis key prefix"`

	// The network type, either tcp or unix.
	// Default is tcp.
	Network string `json:"network" split-default-value:"tcp" split-cli-option:"redis-network" split-cli-description:"Redis network protocol"`

	// Maximum number of retries before giving up.
	// Default is to not retry failed commands.
	MaxRetries int `json:"maxRetries" split-default-value:"0" split-cli-option:"redis-max-retries" split-cli-description:"Redis connection max retries"`

	// Dial timeout for establishing new connections.
	// Default is 5 seconds.
	DialTimeout int `json:"dialTimeout" split-default-value:"5" split-cli-option:"redis-dial-timeout" split-cli-description:"Redis connection dial timeout"`

	// Timeout for socket reads. If reached, commands will fail
	// with a timeout instead of blocking.
	// Default is 10 seconds.
	ReadTimeout int `json:"readTimeout" split-default-value:"10" split-cli-option:"redis-read-timeout" split-cli-description:"Redis connection read timeout"`

	// Timeout for socket writes. If reached, commands will fail
	// with a timeout instead of blocking.
	// Default is 3 seconds.
	WriteTimeout int `json:"writeTimeout" split-default-value:"5" split-cli-option:"redis-write-timeout" split-cli-description:"Redis connection write timeout"`

	// Maximum number of socket connections.
	// Default is 10 connections.
	PoolSize int `json:"poolSize" split-default-value:"10" split-cli-option:"redis-pool" split-cli-description:"Redis connection pool size"`

	// Redis sentinel replication support
	DisableLegacyImpressions bool   `json:"disableLegacyImpressions" split-default-value:"false" split-cli-option:"redis-disable-legacy-impressions" split-cli-description:"Disable looking for legacy impressions"`
	SentinelReplication      bool   `json:"sentinelReplication" split-default-value:"false" split-cli-option:"redis-sentinel-replication" split-cli-description:"Redis sentinel replication enabled."`
	SentinelAddresses        string `json:"sentinelAddresses" split-default-value:"" split-cli-option:"redis-sentinel-addresses" split-cli-description:"List of redis sentinels"`
	SentinelMaster           string `json:"sentinelMaster" split-default-value:"" split-cli-option:"redis-sentinel-master" split-cli-description:"Name of master"`

	// Redis cluster replication support
	ClusterMode           bool     `json:"clusterMode" split-default-value:"false" split-cli-option:"redis-cluster-mode" split-cli-description:"Redis cluster enabled."`
	ClusterNodes          string   `json:"clusterNodes" split-default-value:"" split-cli-option:"redis-cluster-nodes" split-cli-description:"List of redis cluster nodes."`
	ClusterKeyHashTag     string   `json:"keyHashTag" split-default-value:"" split-cli-option:"redis-cluster-key-hashtag" split-cli-description:"keyHashTag for redis cluster."`
	TLS                   bool     `json:"enableTLS" split-default-value:"false" split-cli-option:"redis-tls" split-cli-description:"Use SSL/TLS for connecting to redis"`
	TLSServerName         string   `json:"tlsServerName" split-default-value:"" split-cli-option:"redis-tls-server-name" split-cli-description:"Server name to use when validating a server-side SSL/TLS certificate."`
	TLSCACertificates     []string `json:"caCertificates" split-default-value:"" split-cli-option:"redis-tls-ca-certs" split-cli-description:"Root CA certificates filenames to use when connecting to a redis server via SSL/TLS"`
	TLSSkipNameValidation bool     `json:"tlsSkipNameValidation" split-default-value:"false" split-cli-option:"redis-tls-skip-name-validation" split-cli-description:"Accept server's public key without validanting againsta a CA."`
	TLSClientCertificate  string   `json:"tlsClientCertificate" split-default-value:"" split-cli-option:"redis-tls-client-certificate" split-cli-description:"Client certificate filename signed by a server-recognized CA"`
	TLSClientKey          string   `json:"tlsClientKey" split-default-value:"" split-cli-option:"redis-tls-client-key" split-cli-description:"Client private key matching the certificate."`
	ForceFreshStartup     bool     `json:"forceFreshStartup" split-default-value:"false" split-cli-option:"force-fresh-startup" split-cli-description:"Remove any Split-related data (associated with the specified prefix if any) prior to starting the synchronizer."`
}

// TaskPeriods struct is used to configure the period for each synchronization task
type TaskPeriods struct {
	SplitSync      int
	SegmentSync    int
	ImpressionSync int
	GaugeSync      int
	CounterSync    int
	LatencySync    int
	EventsSync     int
}

// AdvancedConfig exposes more configurable parameters that can be used to further tailor the sdk to the user's needs
// - HTTPTimeout - Timeout for HTTP requests when doing synchronization
// - SegmentQueueSize - How many segments can be queued for updating (should be >= # segments the user has)
// - SegmentWorkers - How many workers will be used when performing segments sync.
type AdvancedConfig struct {
	HTTPTimeout          int
	SegmentQueueSize     int
	SegmentWorkers       int
	SdkURL               string
	EventsURL            string
	EventsBulkSize       int64
	EventsQueueSize      int
	ImpressionsQueueSize int
	ImpressionsBulkSize  int64
}
