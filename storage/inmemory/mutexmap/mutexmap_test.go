//go:build !race
// +build !race

package mutexmap

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v5/dtos"
	"github.com/splitio/go-split-commons/v5/flagsets"
)

func TestSplitMutexMapConcurrency(t *testing.T) {
	splitStorage := NewMMSplitStorage(flagsets.NewFlagSetFilter(nil))
	splits := make([]dtos.SplitDTO, 0, 10)
	for index := 0; index < 10; index++ {
		splits = append(splits, dtos.SplitDTO{
			Name: fmt.Sprintf("SomeSplit_%d", index),
			Algo: index,
		})

	}

	iterations := 100000

	mainWG := sync.WaitGroup{}
	mainWG.Add(iterations)
	go func() {
		for i := 0; i < iterations; i++ {
			go func() {
				time.Sleep(5 * time.Second)
				splitStorage.Update(splits[0:rand.Intn(len(splits)-1)], nil, 123)
				mainWG.Done()
			}()
		}
	}()

	mainWG.Add(iterations)
	go func() {
		for i := 0; i < iterations; i++ {
			go func() {
				time.Sleep(5 * time.Second)
				splitStorage.Split(fmt.Sprintf("SomeSplit_%d", rand.Intn(len(splits)-1)))
				mainWG.Done()
			}()
		}
	}()

	mainWG.Add(iterations)
	go func() {
		for i := 0; i < iterations; i++ {
			go func() {
				time.Sleep(5 * time.Second)
				splitStorage.Remove(fmt.Sprintf("SomeSplit_%d", rand.Intn(len(splits)-1)))
				mainWG.Done()
			}()
		}
	}()

	mainWG.Add(iterations)
	go func() {
		for i := 0; i < iterations; i++ {
			go func() {
				time.Sleep(5 * time.Second)
				splitStorage.SplitNames()
				mainWG.Done()
			}()
		}
	}()

	mainWG.Add(iterations)
	go func() {
		for i := 0; i < iterations; i++ {
			go func() {
				time.Sleep(5 * time.Second)
				splitStorage.SegmentNames()
				mainWG.Done()
			}()
		}
	}()

	mainWG.Add(iterations)
	go func() {
		for i := 0; i < iterations; i++ {
			go func() {
				time.Sleep(5 * time.Second)
				splitStorage.All()
				mainWG.Done()
			}()
		}
	}()

	mainWG.Add(iterations)
	go func() {
		for i := 0; i < iterations; i++ {
			go func() {
				time.Sleep(5 * time.Second)
				splitStorage.ChangeNumber()
				mainWG.Done()
			}()
		}
	}()

	mainWG.Add(iterations)
	go func() {
		for i := 0; i < iterations; i++ {
			go func() {
				time.Sleep(5 * time.Second)
				splitStorage.FetchMany(splitStorage.SplitNames())
				mainWG.Done()
			}()
		}
	}()

	mainWG.Wait()
}
