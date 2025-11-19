package dtos

const (
	labelPrefix = "fallback - "
)

type FallbackTreatment struct {
	Treatment *string
	Config    *string
	Label     *string
}

type FallbackTreatmentConfig struct {
	GlobalFallbackTreatment *FallbackTreatment
	ByFlagFallbackTreatment map[string]FallbackTreatment
}

type FallbackTreatmentCalculator interface {
	Resolve(flagName string, label *string) FallbackTreatment
}

type FallbackTreatmentCalculatorImp struct {
	fallbackTreatmentConfig *FallbackTreatmentConfig
}

func NewFallbackTreatmentCalculatorImp(fallbackTreatmentConfig *FallbackTreatmentConfig) FallbackTreatmentCalculator {
	return &FallbackTreatmentCalculatorImp{
		fallbackTreatmentConfig: fallbackTreatmentConfig,
	}
}

func (f *FallbackTreatmentCalculatorImp) Resolve(flagName string, label *string) FallbackTreatment {
	if f.fallbackTreatmentConfig != nil {
		if byFlag := f.fallbackTreatmentConfig.ByFlagFallbackTreatment; byFlag != nil {
			if val, ok := byFlag[flagName]; ok {
				return FallbackTreatment{
					Treatment: val.Treatment,
					Config:    val.Config,
					Label:     f.resolveLabel(label),
				}
			}
		}
		if f.fallbackTreatmentConfig.GlobalFallbackTreatment != nil {
			return FallbackTreatment{
				Treatment: f.fallbackTreatmentConfig.GlobalFallbackTreatment.Treatment,
				Config:    f.fallbackTreatmentConfig.GlobalFallbackTreatment.Config,
				Label:     f.resolveLabel(label),
			}
		}
	}
	controlTreatment := "control"
	return FallbackTreatment{
		Treatment: &controlTreatment,
		Label:     label,
	}
}

func (f *FallbackTreatmentCalculatorImp) resolveLabel(label *string) *string {
	if label == nil {
		return nil
	}
	result := labelPrefix + *label
	return &result
}
