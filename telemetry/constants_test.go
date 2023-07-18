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
	for _, method := range []string{"track", "Track"} {
		if v, ok := ParseMethodFromRedisHash(method); !ok || v != Track {
			t.Error("expented `track`. Got: ", method)
		}
	}
}
