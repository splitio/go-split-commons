package dtos

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFallbackTreatmentCalculatorResolve(t *testing.T) {
	// Initial setup with both global and flag-specific treatments
	stringConfig := "flag1_config"
	globalTreatment := "global_treatment"
	flag1Treatment := "flag1_treatment"
	config := &FallbackTreatmentConfig{
		GlobalFallbackTreatment: &FallbackTreatment{
			Treatment: &globalTreatment,
		},
		ByFlagFallbackTreatment: map[string]FallbackTreatment{
			"flag1": {
				Treatment: &flag1Treatment,
				Config:    &stringConfig,
			},
		},
	}
	calc := NewFallbackTreatmentCalculatorImp(config)

	// Test flag-specific treatment with label
	label := "some_label"
	result := calc.Resolve("flag1", &label)
	assert.Equal(t, "flag1_treatment", *result.Treatment)
	assert.Equal(t, &stringConfig, result.Config)
	assert.Equal(t, "fallback - some_label", *result.Label)

	// Test fallback to global treatment when flag not found
	result = calc.Resolve("flag2", &label)
	assert.Equal(t, "global_treatment", *result.Treatment)
	assert.Nil(t, result.Config)
	assert.Equal(t, "fallback - some_label", *result.Label)

	// Test nil label handling
	result = calc.Resolve("flag1", nil)
	assert.Equal(t, "flag1_treatment", *result.Treatment)
	assert.Equal(t, &stringConfig, result.Config)
	assert.Nil(t, result.Label)

	// Test default control when no config
	calcNoConfig := NewFallbackTreatmentCalculatorImp(nil)
	result = calcNoConfig.Resolve("flag1", &label)
	assert.Equal(t, "control", *result.Treatment)
	assert.Nil(t, result.Config)
	assert.Equal(t, "some_label", *result.Label)

	// Test global treatment when no flag-specific treatments exist
	configGlobalOnly := &FallbackTreatmentConfig{
		GlobalFallbackTreatment: &FallbackTreatment{
			Treatment: &globalTreatment,
			Config:    nil,
		},
	}
	calcGlobalOnly := NewFallbackTreatmentCalculatorImp(configGlobalOnly)
	result = calcGlobalOnly.Resolve("any_flag", &label)
	assert.Equal(t, "global_treatment", *result.Treatment)
	assert.Nil(t, result.Config)
	assert.Equal(t, "fallback - some_label", *result.Label)
}
