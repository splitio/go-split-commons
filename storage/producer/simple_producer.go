package producer

import (
	"iter"

	"github.com/splitio/go-split-commons/v6/engine/grammar"
	"github.com/splitio/go-split-commons/v6/storage"
	"github.com/splitio/go-toolkit/v5/injection"
	"github.com/splitio/go-toolkit/v5/logging"
)

// Straight forward implementation that does no caching
type SimpleProducer struct {
	splitStorage storage.SplitStorageConsumer
}

func NewSimpleProducer(splitStorage storage.SplitStorageConsumer) *SimpleProducer {
	return &SimpleProducer{splitStorage: splitStorage}
}

func (p *SimpleProducer) GetSplit(splitName string, ctx *injection.Context, logger logging.LoggerInterface) *grammar.Split {
	dto := p.splitStorage.Split(splitName)
	if dto == nil {
		return nil
	}
	return grammar.NewSplit(dto, ctx, logger)
}

func (p *SimpleProducer) GetSplits(splitNames []string, ctx *injection.Context, logger logging.LoggerInterface) iter.Seq2[string, *grammar.Split] {
	return func(yield func(string, *grammar.Split) bool) {
		dtos := p.splitStorage.FetchMany(splitNames)
		for splitName, dto := range dtos {
			var split *grammar.Split
			if dto != nil {
				split = grammar.NewSplit(dto, ctx, logger)
			}
			if !yield(splitName, split) {
				return
			}
		}
	}
}

func (p *SimpleProducer) GetNamesByFlagSets(sets []string) map[string][]string {
	return p.splitStorage.GetNamesByFlagSets(sets)
}

var _ grammar.SplitProducer = (*SimpleProducer)(nil)
