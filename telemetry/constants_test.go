package telemetry

import (
	"testing"
)

func TestMethodMapping(t *testing.T) {
	for _, method := range []string{"treatment", "getTreatment", "get_treatment", "Treatment"} {
		if v, ok := ParseMethodFromRedisHash(method); !ok || v != Treatment {
			t.Error("expented `treatment`. Got: ", v)
		}
	}
	for _, method := range []string{"treatments", "getTreatments", "get_treatments", "Treatments"} {
		if v, ok := ParseMethodFromRedisHash(method); !ok || v != Treatments {
			t.Error("expented `treatments`. Got: ", v)
		}
	}
	for _, method := range []string{"treatmentWithConfig", "getTreatmentWithConfig", "get_treatment_with_config", "TreatmentWithConfig"} {
		if v, ok := ParseMethodFromRedisHash(method); !ok || v != TreatmentWithConfig {
			t.Error("expented `treatmentWithConfig`. Got: ", v)
		}
	}
	for _, method := range []string{"treatmentsWithConfig", "getTreatmentsWithConfig", "get_treatments_with_config", "TreatmentsWithConfig"} {
		if v, ok := ParseMethodFromRedisHash(method); !ok || v != TreatmentsWithConfig {
			t.Error("expented `treatmentsWithConfig`. Got: ", v)
		}
	}
	for _, method := range []string{"treatmentsByFlagSet", "treatments_by_flag_set", "getTreatmentsByFlagSet", "get_treatments_by_flag_set", "TreatmentsByFlagSet"} {
		if v, ok := ParseMethodFromRedisHash(method); !ok || v != TreatmentsByFlagSet {
			t.Error("expected `treatmentsByFlagSet`. Got: ", v)
		}
	}
	for _, method := range []string{"treatmentsByFlagSets", "treatments_by_flag_sets", "getTreatmentsByFlagSets", "get_treatments_by_flag_sets", "TreatmentsByFlagSets"} {
		if v, ok := ParseMethodFromRedisHash(method); !ok || v != TreatmentsByFlagSets {
			t.Error("expected `treatmentsByFlagSets`. Got: ", v)
		}
	}
	for _, method := range []string{"treatmentsWithConfigByFlagSet", "treatments_with_config_by_flag_set", "getTreatmentsWithConfigByFlagSet", "get_treatments_with_config_by_flag_set", "TreatmentsWithConfigByFlagSet"} {
		if v, ok := ParseMethodFromRedisHash(method); !ok || v != TreatmentsWithConfigByFlagSet {
			t.Error("expected `treatmentWithConfigByFlagSet`. Got: ", v)
		}
	}
	for _, method := range []string{"treatmentsWithConfigByFlagSets", "treatments_with_config_by_flag_sets", "getTreatmentsWithConfigByFlagSets", "get_treatments_with_config_by_flag_sets", "TreatmentsWithConfigByFlagSets"} {
		if v, ok := ParseMethodFromRedisHash(method); !ok || v != TreatmentsWithConfigByFlagSets {
			t.Error("expected `treatmentWithConfigByFlagSets`. Got: ", v)
		}
	}
	for _, method := range []string{"track", "Track"} {
		if v, ok := ParseMethodFromRedisHash(method); !ok || v != Track {
			t.Error("expented `track`. Got: ", method)
		}
	}
}
