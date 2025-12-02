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
	for _, method := range []string{"getTreatmentWithEvaluationOptions", "get_treatment_with_evaluation_options", "treatmentWithEvaluationOptions", "TreatmentWithEvaluationOptions"} {
		if v, ok := ParseMethodFromRedisHash(method); !ok || v != TreatmentWithEvaluationOptions {
			t.Error("expected `TreatmentWithEvaluationOptions`. Got: ", v)
		}
	}
	for _, method := range []string{"getTreatmentsWithEvaluationOptions", "get_treatments_with_evaluation_options", "treatmentsWithEvaluationOptions", "TreatmentsWithEvaluationOptions"} {
		if v, ok := ParseMethodFromRedisHash(method); !ok || v != TreatmentsWithEvaluationOptions {
			t.Error("expected `TreatmentsWithEvaluationOptions`. Got: ", v)
		}
	}
	for _, method := range []string{"getTreatmentWithConfigAndEvaluationOptions", "get_treatment_with_config_and_evaluation_options", "treatment_with_config_and_evaluation_options", "treatmentWithConfigAndEvaluationOptions", "TreatmentWithConfigAndEvaluationOptions"} {
		if v, ok := ParseMethodFromRedisHash(method); !ok || v != TreatmentWithConfigAndEvaluationOptions {
			t.Error("expected `TreatmentWithConfigAndEvaluationOptions`. Got: ", v)
		}
	}
	for _, method := range []string{"getTreatmentsWithConfigAndEvaluationOption", "get_treatments_with_config_and_evaluation_options", "treatments_with_config_and_evaluation_options", "treatmentsWithConfigAndEvaluationOptions", "TreatmentsWithConfigAndEvaluationOptions"} {
		if v, ok := ParseMethodFromRedisHash(method); !ok || v != TreatmentsWithConfigAndEvaluationOptions {
			t.Error("expected `TreatmentsWithConfigAndEvaluationOptions`. Got: ", v)
		}
	}
	for _, method := range []string{"getTreatmentsByFlagSetWithEvaluationOptions", "get_treatments_by_flag_set_with_evaluation_options", "treatments_by_flag_set_with_evaluation_options", "treatmentsByFlagSetWithEvaluationOptions", "TreatmentsByFlagSetWithEvaluationOptions"} {
		if v, ok := ParseMethodFromRedisHash(method); !ok || v != TreatmentsByFlagSetWithEvaluationOptions {
			t.Error("expected `TreatmentsByFlagSetWithEvaluationOptions`. Got: ", v)
		}
	}
	for _, method := range []string{"getTreatmentsByFlagSetsWithEvaluationOptions", "get_treatments_by_flag_sets_with_evaluation_options", "treatments_by_flag_sets_with_evaluation_options", "treatmentsByFlagSetsWithEvaluationOptions", "TreatmentsByFlagSetsWithEvaluationOptions"} {
		if v, ok := ParseMethodFromRedisHash(method); !ok || v != TreatmentsByFlagSetsWithEvaluationOptions {
			t.Error("expected `TreatmentsByFlagSetsWithEvaluationOptions`. Got: ", v)
		}
	}
	for _, method := range []string{"getTreatmentsWithConfigByFlagSetAndEvaluationOptions", "get_treatments_with_config_by_flag_set_and_evaluation_options", "treatments_with_config_by_flag_set_and_evaluation_options", "treatmentsWithConfigByFlagSetAndEvaluationOptions", "TreatmentsWithConfigByFlagSetAndEvaluationOptions"} {
		if v, ok := ParseMethodFromRedisHash(method); !ok || v != TreatmentsWithConfigByFlagSetAndEvaluationOptions {
			t.Error("expected `TreatmentsWithConfigByFlagSetAndEvaluationOptions`. Got: ", v)
		}
	}
	for _, method := range []string{"getTreatmentsWithConfigByFlagSetsAndEvaluationOptions", "get_treatments_with_config_by_flag_sets_and_evaluation_options", "treatments_with_config_by_flag_sets_and_evaluation_options", "treatmentsWithConfigByFlagSetsAndEvaluationOptions", "TreatmentsWithConfigByFlagSetsAndEvaluationOptions"} {
		if v, ok := ParseMethodFromRedisHash(method); !ok || v != TreatmentsWithConfigByFlagSetsAndEvaluationOptions {
			t.Error("expected `TreatmentsWithConfigByFlagSetsAndEvaluationOptions`. Got: ", v)
		}
	}
	for _, method := range []string{"track", "Track"} {
		if v, ok := ParseMethodFromRedisHash(method); !ok || v != Track {
			t.Error("expented `track`. Got: ", method)
		}
	}
}
