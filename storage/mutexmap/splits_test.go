package mutexmap

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/dtos"
)

func TestMMSplitStorage(t *testing.T) {
	splitStorage := NewMMSplitStorage()

	result := splitStorage.All()
	if len(result) != 0 {
		t.Error("Unexpected number of splits returned")
	}

	splits := make([]dtos.SplitDTO, 0, 10)
	for index := 0; index < 10; index++ {
		splits = append(splits, dtos.SplitDTO{
			Name: fmt.Sprintf("SomeSplit_%d", index),
			Algo: index,
		})
	}
	splitStorage.PutMany(splits, 123)

	for index := 0; index < 10; index++ {
		splitName := fmt.Sprintf("SomeSplit_%d", index)
		split := splitStorage.Split(splitName)
		if split == nil || split.Name != splitName || split.Algo != index {
			t.Error("Split not returned as expected")
		}
	}

	result = splitStorage.All()
	if len(result) != 10 {
		t.Error("Unexpected number of splits returned")
	}

	splitNames := make([]string, 0)
	for index := 0; index < 10; index++ {
		splitNames = append(splitNames, fmt.Sprintf("SomeSplit_%d", index))
	}
	splitsFetchedMany := splitStorage.FetchMany(splitNames)
	if len(splitsFetchedMany) != 10 {
		t.Error("It should return 10 splits")
	}
	for index := 0; index < 10; index++ {
		if splitsFetchedMany[fmt.Sprintf("SomeSplit_%d", index)] == nil {
			t.Error("It should not be nil")
		}
	}
	splitsFetchedMany = splitStorage.FetchMany([]string{"nonexistent_split"})
	if splitsFetchedMany["nonexistent_split"] != nil {
		t.Error("It should be nil")
	}

	split := splitStorage.Split("nonexistent_split")
	if split != nil {
		t.Error("Nil expected but split returned")
	}

	splitStorage.Remove("SomeSplit_7")
	for index := 0; index < 10; index++ {
		splitName := fmt.Sprintf("SomeSplit_%d", index)
		split := splitStorage.Split(splitName)
		if index == 7 {
			if split != nil {
				t.Error("Split Should have been removed and is present")
			}
		} else {
			if split == nil || split.Name != splitName || split.Algo != index {
				t.Error("Split should not have been removed or modified and it was")
			}
		}
	}
}

func TestSplitMutexMapConcurrency(t *testing.T) {
	splitStorage := NewMMSplitStorage()
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
				splitStorage.PutMany(splits[0:rand.Intn(len(splits)-1)], 123)
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

func TestTrafficTypeOnUpdates(t *testing.T) {
	s1 := dtos.SplitDTO{
		Name:            "s1",
		TrafficTypeName: "tt1",
	}

	splitStorage := NewMMSplitStorage()
	splitStorage.PutMany([]dtos.SplitDTO{s1}, 123)

	if !splitStorage.TrafficTypeExists("tt1") {
		t.Error("Traffic type 1 should exist.")
	}

	if splitStorage.TrafficTypeExists("tt2") {
		t.Error("Traffic type 2 should not exist.")
	}

	s1.TrafficTypeName = "tt2"
	splitStorage.PutMany([]dtos.SplitDTO{s1}, 123)
	if splitStorage.TrafficTypeExists("tt1") {
		t.Error("Traffic type 1 should not exist.")
	}

	if !splitStorage.TrafficTypeExists("tt2") {
		t.Error("Traffic type 2 should exist.")
	}
}

func TestTrafficTypes(t *testing.T) {
	ttStorage := NewMMSplitStorage()

	if ttStorage.TrafficTypeExists("mytest") {
		t.Error("It should not exist")
	}

	ttStorage.increaseTrafficTypeCount("mytest")
	if !ttStorage.TrafficTypeExists("mytest") {
		t.Error("It should exist")
	}

	ttStorage.decreaseTrafficTypeCount("mytest")
	if ttStorage.TrafficTypeExists("mytest") {
		t.Error("It should not exist")
	}
}
