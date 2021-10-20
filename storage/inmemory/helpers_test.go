package inmemory

import "testing"

func TestAtomicSlice(t *testing.T) {
	_, err := NewAtomicInt64Slice(-1)
	if err == nil || err.Error() != "invalid array size: -1" {
		t.Error("It should return err")
	}

	forTest, err := NewAtomicInt64Slice(10)
	if err != nil {
		t.Error("err should be nil")
	}

	if len(forTest) != 10 {
		t.Error("Wrong size")
	}

	forTest.Incr(0)
	forTest.Incr(1)
	forTest.Incr(1)

	_, err = forTest.FetchAndClearOne(10)
	if err == nil || err.Error() != "out of bounds" {
		t.Error("It should return err")
	}

	val, _ := forTest.FetchAndClearOne(0)
	if val != 1 {
		t.Error("Unexpected value")
	}

	val, _ = forTest.FetchAndClearOne(0)
	if val != 0 {
		t.Error("Unexpected value")
	}

	val, _ = forTest.FetchAndClearOne(5)
	if val != 0 {
		t.Error("Unexpected value")
	}

	arr := forTest.FetchAndClearAll()
	if len(arr) != 10 {
		t.Error("Wrong size")
	}

	if arr[1] != 2 {
		t.Error("Unexpected value")
	}
}
