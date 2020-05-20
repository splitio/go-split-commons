package sync

import (
	"github.com/splitio/go-split-commons/conf"
	"github.com/splitio/go-split-commons/service/api"
)

// SynchronizerManager struct
type SynchronizerManager struct {
	apikey       string
	synchronizer Synchronizer
}

// Start starts synchronization through Split
func (s *SynchronizerManager) Start() error {
	err := api.ValidateApikey(s.apikey, conf.AdvancedConfig{})
	if err != nil {
		return err
	}

	return nil
}
