package dtos

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFFResponseLegacy(t *testing.T) {
	some, err := NewFFResponseLegacy([]byte(`{wrong json}`))
	assert.Nil(t, some)
	assert.NotNil(t, err)

	ok, err := NewFFResponseLegacy([]byte(`{"since":1,"till":2,"splits":[{"name":"split1"},{"name":"split2"}]}`))
	assert.NotNil(t, ok)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), ok.FFTill())
	assert.Equal(t, int64(0), ok.RBTill())
	assert.Equal(t, int64(1), ok.FFSince())
	assert.Equal(t, int64(0), ok.RBSince())
	assert.False(t, ok.NeedsAnotherFetch())
	assert.Len(t, ok.FeatureFlags(), 2)
	assert.Empty(t, ok.RuleBasedSegments())

	noFetch, err := NewFFResponseLegacy([]byte(`{"since":2,"till":2,"splits":[]}`))
	assert.NotNil(t, noFetch)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), noFetch.FFTill())
	assert.Equal(t, int64(0), noFetch.RBTill())
	assert.Equal(t, int64(2), noFetch.FFSince())
	assert.Equal(t, int64(0), noFetch.RBSince())
	assert.True(t, noFetch.NeedsAnotherFetch())
	assert.Len(t, noFetch.FeatureFlags(), 0)
	assert.Empty(t, noFetch.RuleBasedSegments())
}

func TestFFResponseLatest(t *testing.T) {
	some, err := NewFFResponseV13([]byte(`{wrong json}`))
	assert.Nil(t, some)
	assert.NotNil(t, err)

	ok, err := NewFFResponseV13([]byte(`{
		"ff":{
			"s":1,
			"t":2,
			"d":[{"name":"split1"},{"name":"split2"}]
		},
		"rbs":{
			"s":3,
			"t":4,
			"d":[{"name":"rb1"},{"name":"rb2"},{"name":"rb3"}]
		}
	}`))
	assert.NotNil(t, ok)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), ok.FFTill())
	assert.Equal(t, int64(4), ok.RBTill())
	assert.Equal(t, int64(1), ok.FFSince())
	assert.Equal(t, int64(3), ok.RBSince())
	assert.Len(t, ok.FeatureFlags(), 2)
	assert.Len(t, ok.RuleBasedSegments(), 3)
	assert.False(t, ok.NeedsAnotherFetch())

	noFetch, err := NewFFResponseV13([]byte(`{
		"ff":{
			"s":2,
			"t":2,
			"d":[]
		},
		"rbs":{
			"s":4,
			"t":4,
			"d":[]
		}
	}`))
	assert.NotNil(t, noFetch)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), noFetch.FFTill())
	assert.Equal(t, int64(4), noFetch.RBTill())
	assert.Equal(t, int64(2), noFetch.FFSince())
	assert.Equal(t, int64(4), noFetch.RBSince())
	assert.Len(t, noFetch.FeatureFlags(), 0)
	assert.Len(t, noFetch.RuleBasedSegments(), 0)
	assert.True(t, noFetch.NeedsAnotherFetch())
}

func TestFFResponseLocalV13(t *testing.T) {
	some, err := NewFFResponseLocalV13([]byte(`{wrong json}`))
	assert.Nil(t, some)
	assert.NotNil(t, err)

	ok, err := NewFFResponseLocalV13([]byte(`{
		"ff":{
			"s":1,
			"t":2,
			"d":[{"name":"split1"},{"name":"split2"}]
		},
		"rbs":{
			"s":3,
			"t":4,
			"d":[{"name":"rb1"},{"name":"rb2"},{"name":"rb3"}]
		}
	}`))
	assert.NotNil(t, ok)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), ok.FFTill())
	assert.Equal(t, int64(4), ok.RBTill())
	assert.Equal(t, int64(1), ok.FFSince())
	assert.Equal(t, int64(3), ok.RBSince())
	assert.Len(t, ok.FeatureFlags(), 2)
	assert.Len(t, ok.RuleBasedSegments(), 3)
	assert.False(t, ok.NeedsAnotherFetch())

	ok.SetFFTill(5)
	ok.SetFFSince(5)
	ok.SetRBSince(5)
	ok.SetRBTill(5)
	assert.Equal(t, int64(5), ok.FFTill())
	assert.Equal(t, int64(5), ok.RBTill())
	assert.Equal(t, int64(5), ok.FFSince())
	assert.Equal(t, int64(5), ok.RBSince())
	ok.ReplaceFF([]SplitDTO{{Name: "other"}})
	ok.ReplaceRB([]RuleBasedSegmentDTO{{Name: "other_rb"}})
	assert.Len(t, ok.FeatureFlags(), 1)
	assert.Len(t, ok.RuleBasedSegments(), 1)
	assert.Equal(t, "other", ok.FeatureFlags()[0].Name)
	assert.Equal(t, "other_rb", ok.RuleBasedSegments()[0].Name)

	noFetch, err := NewFFResponseLocalV13([]byte(`{
		"ff":{
			"s":2,
			"t":2,
			"d":[]
		},
		"rbs":{
			"s":4,
			"t":4,
			"d":[]
		}
	}`))
	assert.NotNil(t, noFetch)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), noFetch.FFTill())
	assert.Equal(t, int64(4), noFetch.RBTill())
	assert.Equal(t, int64(2), noFetch.FFSince())
	assert.Equal(t, int64(4), noFetch.RBSince())
	assert.Len(t, noFetch.FeatureFlags(), 0)
	assert.Len(t, noFetch.RuleBasedSegments(), 0)
	assert.True(t, noFetch.NeedsAnotherFetch())
}
