package push

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestUpdateManagers(t *testing.T) {
	keeper := NewKeeper(make(chan int, 5))

	for i := 0; i < 5; i++ {
		keeper.UpdateManagers(fmt.Sprintf("some_%d", i), i)
	}

	if len(keeper.managers) != 5 {
		t.Error("Unexpected managers registered")
	}
	if keeper.Publishers("unexistent") != nil {
		t.Error("It should be nil")
	}
	if *keeper.Publishers("some_0") != 0 {
		t.Error("It should be one")
	}
	if *keeper.Publishers("some_4") != 4 {
		t.Error("It should be five")
	}
}

func TestUpdateLastNotification(t *testing.T) {
	keeper := NewKeeper(make(chan int, 5))

	for i := 0; i < 5; i++ {
		keeper.UpdateLastNotification(fmt.Sprintf("some_%d", i), int64(i))
	}

	manager, timestamp := keeper.LastNotification()
	if manager != "some_4" {
		t.Error("Unexpected manager registered for latest")
	}
	if timestamp != 4 {
		t.Error("Unexpected manager registered for latest")
	}
}

func TestKeeperMultipleOps(t *testing.T) {
	iterations := 3000
	keeper := NewKeeper(make(chan int, 3000))

	mainWG := sync.WaitGroup{}
	mainWG.Add(iterations)
	go func() {
		for i := 0; i < iterations; i++ {
			go func() {
				time.Sleep(5 * time.Second)
				keeper.UpdateManagers(fmt.Sprintf("some_%d", rand.Int()), rand.Int())
				mainWG.Done()
			}()
		}
	}()

	mainWG.Add(iterations)
	go func() {
		for i := 0; i < iterations; i++ {
			go func() {
				time.Sleep(5 * time.Second)
				keeper.UpdateLastNotification(fmt.Sprintf("some_%d", rand.Int()), int64(rand.Int()))
				mainWG.Done()
			}()
		}
	}()
}
