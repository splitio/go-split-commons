package util

import "testing"

func TestFlagSetFilter(t *testing.T) {
	filter := NewFlagSetFilter([]string{})
	if !filter.Match([]string{}) {
		t.Error("It should be true")
	}
	if !filter.Match([]string{"one"}) {
		t.Error("It should be true")
	}

	filter = NewFlagSetFilter([]string{"one", "two"})
	if filter.Match([]string{}) {
		t.Error("It should be false")
	}
	if !filter.Match([]string{"one"}) {
		t.Error("It should be true")
	}
	if !filter.Match([]string{"three", "one"}) {
		t.Error("It should be true")
	}
	if filter.Match([]string{"three", "four"}) {
		t.Error("It should be false")
	}
}
