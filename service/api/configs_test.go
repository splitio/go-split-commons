package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/splitio/go-split-commons/v9/conf"
	"github.com/splitio/go-split-commons/v9/dtos"
	"github.com/splitio/go-split-commons/v9/service"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/stretchr/testify/assert"
)

func TestHTTPConfigsFetcherFetchSuccess(t *testing.T) {
	logger := logging.NewLogger(nil)

	// Create mock response
	mockResponse := dtos.ConfigsResponseDTO{
		Updated: []dtos.ConfigDTO{
			{
				Name:            "config1",
				Status:          "ACTIVE",
				Killed:          false,
				TrafficTypeName: "user",
				ChangeNumber:    100,
				Sets:            []string{},
				Variants: []dtos.VariantDTO{
					{
						Name:       "on",
						Definition: map[string]interface{}{"color": "blue"},
					},
				},
				Targeting: dtos.TargetingDTO{
					Default:               "on",
					Seed:                  777,
					TrafficAllocation:     100,
					TrafficAllocationSeed: 999,
					Conditions: []dtos.RawConditionDTO{
						{
							Label: "custom",
							Partitions: []dtos.RawPartitionDTO{
								{
									Variant: "on",
									Size:    100,
								},
							},
							Matchers: []dtos.RawMatcherDTO{
								{
									Type: "ALL_KEYS",
								},
							},
						},
					},
				},
			},
			{
				Name:         "config2",
				ChangeNumber: 200,
				Targeting: dtos.TargetingDTO{
					Default: "off",
					Seed:    888,
				},
			},
		},
		Since: 123,
		Till:  456,
		RBS: []dtos.RuleBasedSegmentDTO{
			{
				Name:         "segment1",
				ChangeNumber: 300,
			},
		},
	}

	responseJSON, _ := json.Marshal(mockResponse)

	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/configs", r.URL.Path)
		assert.Equal(t, "123", r.URL.Query().Get("since"))
		w.WriteHeader(http.StatusOK)
		w.Write(responseJSON)
	}))
	defer server.Close()

	// Create fetcher
	cfg := conf.AdvancedConfig{
		SdkURL:      server.URL,
		HTTPTimeout: 10,
	}
	metadata := dtos.Metadata{}
	fetcher := NewHTTPConfigsFetcher("test-api-key", cfg, logger, metadata)

	// Create fetch options
	fetchOptions := service.MakeFlagRequestParams().WithChangeNumber(123)

	// Execute fetch
	result, err := fetcher.Fetch(fetchOptions)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Verify FFResponse methods
	assert.Equal(t, int64(123), result.FFSince())
	assert.Equal(t, int64(456), result.FFTill())
	assert.Equal(t, int64(123), result.RBSince())
	assert.Equal(t, int64(456), result.RBTill())
	assert.False(t, result.NeedsAnotherFetch())

	// Verify FeatureFlags
	splits := result.FeatureFlags()
	assert.Equal(t, 2, len(splits))

	// Verify first split (with all fields)
	split1 := splits[0]
	assert.Equal(t, "config1", split1.Name)
	assert.Equal(t, "ACTIVE", split1.Status)
	assert.Equal(t, false, split1.Killed)
	assert.Equal(t, "user", split1.TrafficTypeName)
	assert.Equal(t, "on", split1.DefaultTreatment)
	assert.Equal(t, int64(100), split1.ChangeNumber)
	assert.Equal(t, 100, split1.TrafficAllocation)
	assert.Equal(t, int64(999), split1.TrafficAllocationSeed)
	assert.Equal(t, int64(777), split1.Seed)
	assert.Equal(t, 2, split1.Algo)
	assert.NotNil(t, split1.Configurations)
	// Should have 2 conditions: 1 from targeting + 1 default rule
	assert.Equal(t, 2, len(split1.Conditions))

	// Verify second split (with defaults)
	split2 := splits[1]
	assert.Equal(t, "config2", split2.Name)
	assert.Equal(t, "ACTIVE", split2.Status)        // Default
	assert.Equal(t, "user", split2.TrafficTypeName) // Default
	assert.Equal(t, "off", split2.DefaultTreatment)
	assert.Equal(t, int64(200), split2.ChangeNumber)
	assert.Equal(t, int64(888), split2.Seed)
	assert.Equal(t, 2, split2.Algo)
	assert.Equal(t, 1, len(split2.Conditions)) // Default condition created

	// Verify RuleBasedSegments
	rbs := result.RuleBasedSegments()
	assert.Equal(t, 1, len(rbs))
	assert.Equal(t, "segment1", rbs[0].Name)
	assert.Equal(t, int64(300), rbs[0].ChangeNumber)
}

func TestHTTPConfigsFetcherFetchEmptyConfigs(t *testing.T) {
	logger := logging.NewLogger(nil)

	// Create mock response with no configs
	mockResponse := dtos.ConfigsResponseDTO{
		Updated: []dtos.ConfigDTO{},
		Since:   100,
		Till:    200,
		RBS:     []dtos.RuleBasedSegmentDTO{},
	}

	responseJSON, _ := json.Marshal(mockResponse)

	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(responseJSON)
	}))
	defer server.Close()

	// Create fetcher
	cfg := conf.AdvancedConfig{
		SdkURL:      server.URL,
		HTTPTimeout: 10,
	}
	metadata := dtos.Metadata{}
	fetcher := NewHTTPConfigsFetcher("test-api-key", cfg, logger, metadata)

	// Create fetch options
	fetchOptions := service.MakeFlagRequestParams().WithChangeNumber(100)

	// Execute fetch
	result, err := fetcher.Fetch(fetchOptions)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(100), result.FFSince())
	assert.Equal(t, int64(200), result.FFTill())
	assert.Equal(t, 0, len(result.FeatureFlags()))
	assert.Equal(t, 0, len(result.RuleBasedSegments()))
}

func TestHTTPConfigsFetcherFetchHTTPError(t *testing.T) {
	logger := logging.NewLogger(nil)

	// Create test server that returns error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
	}))
	defer server.Close()

	// Create fetcher
	cfg := conf.AdvancedConfig{
		SdkURL:      server.URL,
		HTTPTimeout: 10,
	}
	metadata := dtos.Metadata{}
	fetcher := NewHTTPConfigsFetcher("test-api-key", cfg, logger, metadata)

	// Create fetch options
	fetchOptions := service.MakeFlagRequestParams().WithChangeNumber(123)

	// Execute fetch
	result, err := fetcher.Fetch(fetchOptions)

	// Assertions
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestHTTPConfigsFetcherFetchInvalidJSON(t *testing.T) {
	logger := logging.NewLogger(nil)

	// Create test server that returns invalid JSON
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("invalid json"))
	}))
	defer server.Close()

	// Create fetcher
	cfg := conf.AdvancedConfig{
		SdkURL:      server.URL,
		HTTPTimeout: 10,
	}
	metadata := dtos.Metadata{}
	fetcher := NewHTTPConfigsFetcher("test-api-key", cfg, logger, metadata)

	// Create fetch options
	fetchOptions := service.MakeFlagRequestParams().WithChangeNumber(123)

	// Execute fetch
	result, err := fetcher.Fetch(fetchOptions)

	// Assertions
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestHTTPConfigsFetcherFetchWithDefaultConditions(t *testing.T) {
	logger := logging.NewLogger(nil)

	// Create mock response with config that has no conditions
	mockResponse := dtos.ConfigsResponseDTO{
		Updated: []dtos.ConfigDTO{
			{
				Name:         "config_no_conditions",
				ChangeNumber: 100,
				Targeting: dtos.TargetingDTO{
					Default: "control",
					Seed:    555,
				},
			},
		},
		Since: 1,
		Till:  2,
		RBS:   []dtos.RuleBasedSegmentDTO{},
	}

	responseJSON, _ := json.Marshal(mockResponse)

	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(responseJSON)
	}))
	defer server.Close()

	// Create fetcher
	cfg := conf.AdvancedConfig{
		SdkURL:      server.URL,
		HTTPTimeout: 10,
	}
	metadata := dtos.Metadata{}
	fetcher := NewHTTPConfigsFetcher("test-api-key", cfg, logger, metadata)

	// Create fetch options
	fetchOptions := service.MakeFlagRequestParams().WithChangeNumber(1)

	// Execute fetch
	result, err := fetcher.Fetch(fetchOptions)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, result)

	splits := result.FeatureFlags()
	assert.Equal(t, 1, len(splits))

	split := splits[0]
	assert.Equal(t, "config_no_conditions", split.Name)
	assert.Equal(t, "control", split.DefaultTreatment)

	// Verify default condition was created
	assert.Equal(t, 1, len(split.Conditions))
	assert.Equal(t, "ROLLOUT", split.Conditions[0].ConditionType)
	assert.Equal(t, "default rule", split.Conditions[0].Label)
	assert.Equal(t, "control", split.Conditions[0].Partitions[0].Treatment)
	assert.Equal(t, 100, split.Conditions[0].Partitions[0].Size)
}

func TestHTTPConfigsFetcherIsProxy(t *testing.T) {
	logger := logging.NewLogger(nil)
	cfg := conf.AdvancedConfig{
		SdkURL:      "http://localhost",
		HTTPTimeout: 10,
	}
	metadata := dtos.Metadata{}
	fetcher := NewHTTPConfigsFetcher("test-api-key", cfg, logger, metadata)

	assert.False(t, fetcher.IsProxy())
}

func TestHTTPConfigsFetcherImplementsSplitFetcher(t *testing.T) {
	logger := logging.NewLogger(nil)
	cfg := conf.AdvancedConfig{
		SdkURL:      "http://localhost",
		HTTPTimeout: 10,
	}
	metadata := dtos.Metadata{}
	fetcher := NewHTTPConfigsFetcher("test-api-key", cfg, logger, metadata)

	// Verify it implements the interface
	var _ service.SplitFetcher = fetcher
	assert.NotNil(t, fetcher)
}
