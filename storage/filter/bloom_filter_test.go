package filter

import (
	"fmt"
	"testing"
)

func TestBloomFilter(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	key := "feature-name-"
	for i := 0; i < 10; i++ {
		bf.Add(key + fmt.Sprint(i))
	}

	for i := 0; i < 10; i++ {
		if !bf.Contains(key + fmt.Sprint(i)) {
			t.Error("Filter must contain the key")
		}
	}

	for i := 10; i < 20; i++ {
		if bf.Contains(key + fmt.Sprint(i)) {
			t.Error("Filter must not contain the key")
		}
	}

	bf.Clear()

	for i := 0; i < 10; i++ {
		if bf.Contains(key + fmt.Sprint(i)) {
			t.Error("Filter must not contain the key")
		}
	}
}
