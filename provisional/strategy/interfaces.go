package strategy

import "github.com/splitio/go-split-commons/v5/dtos"

// ProcessStrategyInterface interface
type ProcessStrategyInterface interface {
	Apply(impressions []dtos.Impression) ([]dtos.Impression, []dtos.Impression)
	ApplySingle(impression *dtos.Impression) bool
}
