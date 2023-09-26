package flagsets

import "testing"

func TestFlagSetFilter(t *testing.T) {
	filter := NewFlagSetFilter([]string{})
	if !filter.IsPresent("one") {
		t.Error("It should be true")
	}
	if !filter.Instersect([]string{}) {
		t.Error("It should be true")
	}
	if !filter.Instersect([]string{"one"}) {
		t.Error("It should be true")
	}

	filter = NewFlagSetFilter([]string{"one", "two"})
	if !filter.IsPresent("one") {
		t.Error("It should be true")
	}
	if filter.IsPresent("three") {
		t.Error("It should be false")
	}
	if filter.Instersect([]string{}) {
		t.Error("It should be false")
	}
	if !filter.Instersect([]string{"one"}) {
		t.Error("It should be true")
	}
	if !filter.Instersect([]string{"three", "one"}) {
		t.Error("It should be true")
	}
	if filter.Instersect([]string{"three", "four"}) {
		t.Error("It should be false")
	}
}
