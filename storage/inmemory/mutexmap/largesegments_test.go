package mutexmap

import (
	"fmt"
	"sort"
	"testing"
)

func sortedKeys(prefix string, count int, shared *string) []string {
	keys := make([]string, 0, count)
	for i := 0; i < count; i++ {
		keys = append(keys, fmt.Sprintf("%s-user-id-%d", prefix, i))
	}

	if shared != nil {
		keys = append(keys, *shared)
	}

	sort.Strings(keys)
	return keys
}

func TestLargeSegmentStorage(t *testing.T) {
	storage := NewLargeSegmentsStorage()

	keys1 := sortedKeys("ls1", 10000, nil)
	storage.Update("ls_test_1", keys1, 1)

	sharedKey := &keys1[5000]
	keys2 := sortedKeys("ls2", 20000, sharedKey)
	storage.Update("ls_test_2", keys2, 2)

	keys3 := sortedKeys("ls3", 30000, sharedKey)
	storage.Update("ls_test_3", keys3, 3)

	if storage.Count() != 3 {
		t.Error("Count should be 3. Actual: ", storage.Count())
	}

	result := storage.LargeSegmentsForUser(*sharedKey)
	sort.Strings(result)
	if result[0] != "ls_test_1" || result[1] != "ls_test_2" || result[2] != "ls_test_3" {
		t.Error("Count should be 3. Actual: ", result)
	}

	result = storage.LargeSegmentsForUser(keys1[100])
	if len(result) != 1 || result[0] != "ls_test_1" {
		t.Error("Count should be 1. Actual: ", result)
	}

	result = storage.LargeSegmentsForUser(keys2[100])
	if len(result) != 1 || result[0] != "ls_test_2" {
		t.Error("Count should be 1. Actual: ", result)
	}

	result = storage.LargeSegmentsForUser(keys3[100])
	if len(result) != 1 || result[0] != "ls_test_3" {
		t.Error("Count should be 1. Actual: ", result)
	}

	result = storage.LargeSegmentsForUser("mauro-test")
	if len(result) != 0 {
		t.Error("Count should be empty. Actual: ", result)
	}
}

func TestChangeNumber(t *testing.T) {
	storage := NewLargeSegmentsStorage()
	lsName := "largeSegment"

	storage.SetChangeNumber(lsName, 1001)
	result := storage.ChangeNumber(lsName)
	if result != 1001 {
		t.Error("ChangeNumber should be 1001. Actual: ", result)
	}

	otherLS := "otherLargeSegment"
	storage.SetChangeNumber(otherLS, 2002)
	result = storage.ChangeNumber(otherLS)
	if result != 2002 {
		t.Error("ChangeNumber should be 2002. Actual: ", result)
	}

	storage.SetChangeNumber(lsName, 1002)
	result = storage.ChangeNumber(lsName)
	if result != 1002 {
		t.Error("ChangeNumber should be 1002. Actual: ", result)
	}
}

func TestContainsKey(t *testing.T) {
	storage := NewLargeSegmentsStorage()

	lsName := "ls_test_1"
	keys1 := sortedKeys("ls1", 10000, nil)
	storage.Update(lsName, keys1, 1)

	exists, err := storage.ContainsKey(lsName, keys1[500])
	if err != nil {
		t.Error("error should be nil. Actual", err.Error())
	}
	if !exists {
		t.Errorf("the key %s should exists", keys1[500])
	}

	exists, err = storage.ContainsKey(lsName, "wrong-key")
	if err != nil {
		t.Error("error should be nil. Actual", err.Error())
	}
	if exists {
		t.Error("the key wrong-key should not exists")
	}

	exists, err = storage.ContainsKey("wrong-key-lsname", keys1[500])
	if err == nil {
		t.Error("error should not be nil. Actual", err)
	}
	if exists {
		t.Error("exists should be false")
	}
}
