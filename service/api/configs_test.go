package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/splitio/go-split-commons/v9/conf"
	"github.com/splitio/go-split-commons/v9/dtos"
	"github.com/splitio/go-toolkit/v5/logging"
	"github.com/stretchr/testify/assert"
)

func TestHTTPConfigsFetcher_Fetch_Success(t *testing.T) {
	logger := logging.NewLogger(nil)

	// Create mock response
	mockResponse := dtos.ConfigsResponseDTO{
		Configs: dtos.ConfigsDataDTO{
			Since: 123,
			Till:  456,
			Configs: []dtos.ConfigDTO{
				{
					Name:                  "config1",
					Status:                "ACTIVE",
					Killed:                false,
					TrafficTypeName:       "user",
					DefaultTreatment:      "on",
					ChangeNumber:          100,
					TrafficAllocation:     100,
					TrafficAllocationSeed: 999,
					Seed:                  777,
					Configurations: map[string]string{
						"on": "{\"color\": \"blue\"}",
					},
					Conditions: []dtos.ConditionDTO{
						{
							ConditionType: "ROLLOUT",
							Label:         "custom",
							MatcherGroup: dtos.MatcherGroupDTO{
								Combiner: "AND",
								Matchers: []dtos.MatcherDTO{
									{
										MatcherType: "ALL_KEYS",
										Negate:      false,
									},
								},
							},
							Partitions: []dtos.PartitionDTO{
								{
									Treatment: "on",
									Size:      100,
								},
							},
						},
					},
				},
				{
					Name:             "config2",
					DefaultTreatment: "off",
					ChangeNumber:     200,
					Seed:             888,
				},
			},
		},
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

	// Execute fetch
	result, err := fetcher.Fetch(123)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Verify FeatureFlags
	assert.Equal(t, int64(123), result.FeatureFlags.Since)
	assert.Equal(t, int64(456), result.FeatureFlags.Till)
	assert.Equal(t, 2, len(result.FeatureFlags.Splits))

	// Verify first split (with all fields)
	split1 := result.FeatureFlags.Splits[0]
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
	assert.Equal(t, 1, len(split1.Conditions))

	// Verify second split (with defaults)
	split2 := result.FeatureFlags.Splits[1]
	assert.Equal(t, "config2", split2.Name)
	assert.Equal(t, "ACTIVE", split2.Status) // Default
	assert.Equal(t, "user", split2.TrafficTypeName) // Default
	assert.Equal(t, "off", split2.DefaultTreatment)
	assert.Equal(t, int64(200), split2.ChangeNumber)
	assert.Equal(t, int64(888), split2.Seed)
	assert.Equal(t, 2, split2.Algo)
	assert.Equal(t, 1, len(split2.Conditions)) // Default condition created

	// Verify RuleBasedSegments
	assert.Equal(t, int64(123), result.RuleBasedSegments.Since)
	assert.Equal(t, int64(456), result.RuleBasedSegments.Till)
	assert.Equal(t, 1, len(result.RuleBasedSegments.RuleBasedSegments))
	assert.Equal(t, "segment1", result.RuleBasedSegments.RuleBasedSegments[0].Name)
	assert.Equal(t, int64(300), result.RuleBasedSegments.RuleBasedSegments[0].ChangeNumber)
}

func TestHTTPConfigsFetcher_Fetch_EmptyConfigs(t *testing.T) {
	logger := logging.NewLogger(nil)

	// Create mock response with no configs
	mockResponse := dtos.ConfigsResponseDTO{
		Configs: dtos.ConfigsDataDTO{
			Since:   100,
			Till:    200,
			Configs: []dtos.ConfigDTO{},
		},
		RBS: []dtos.RuleBasedSegmentDTO{},
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

	// Execute fetch
	result, err := fetcher.Fetch(100)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(100), result.FeatureFlags.Since)
	assert.Equal(t, int64(200), result.FeatureFlags.Till)
	assert.Equal(t, 0, len(result.FeatureFlags.Splits))
	assert.Equal(t, 0, len(result.RuleBasedSegments.RuleBasedSegments))
}

func TestHTTPConfigsFetcher_Fetch_HTTPError(t *testing.T) {
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

	// Execute fetch
	result, err := fetcher.Fetch(123)

	// Assertions
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestHTTPConfigsFetcher_Fetch_InvalidJSON(t *testing.T) {
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

	// Execute fetch
	result, err := fetcher.Fetch(123)

	// Assertions
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestHTTPConfigsFetcher_Fetch_WithDefaultConditions(t *testing.T) {
	logger := logging.NewLogger(nil)

	// Create mock response with config that has no conditions
	mockResponse := dtos.ConfigsResponseDTO{
		Configs: dtos.ConfigsDataDTO{
			Since: 1,
			Till:  2,
			Configs: []dtos.ConfigDTO{
				{
					Name:             "config_no_conditions",
					DefaultTreatment: "control",
					ChangeNumber:     100,
					Seed:             555,
				},
			},
		},
		RBS: []dtos.RuleBasedSegmentDTO{},
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

	// Execute fetch
	result, err := fetcher.Fetch(1)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 1, len(result.FeatureFlags.Splits))

	split := result.FeatureFlags.Splits[0]
	assert.Equal(t, "config_no_conditions", split.Name)
	assert.Equal(t, "control", split.DefaultTreatment)

	// Verify default condition was created
	assert.Equal(t, 1, len(split.Conditions))
	assert.Equal(t, "ROLLOUT", split.Conditions[0].ConditionType)
	assert.Equal(t, "default rule", split.Conditions[0].Label)
	assert.Equal(t, "control", split.Conditions[0].Partitions[0].Treatment)
	assert.Equal(t, 100, split.Conditions[0].Partitions[0].Size)
}
