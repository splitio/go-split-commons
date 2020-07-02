package push

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestUpdateManagers(t *testing.T) {
	keeper := NewKeeper(make(chan int, 1))

	for i := 0; i < 5; i++ {
		keeper.UpdateManagers(fmt.Sprintf("some_%d", i), i)
	}

	if len(keeper.managers) != 5 {
		t.Error("Unexpected managers registered")
	}
}

func TestUpdateLastNotification(t *testing.T) {
	keeper := NewKeeper(make(chan int, 1))

	for i := 0; i < 5; i++ {
		keeper.UpdateLastNotification(fmt.Sprintf("some_%d", i), int64(i))
	}

	if keeper.last.manager != "some_4" {
		t.Error("Unexpected manager registered for latest")
	}

	if keeper.last.timestamp != 4 {
		t.Error("Unexpected manager registered for latest")
	}
}

func TestKeeperMultipleOps(t *testing.T) {
	iterations := 3000
	keeper := NewKeeper(make(chan int, 1))

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
