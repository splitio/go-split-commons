package largesegment

import "testing"

func TestSynchronizeLargeSegment(t *testing.T) {
	updater := NewNoOpLargeSegmentUpdater()

	err := updater.SynchronizeLargeSegment("laName", nil)
	if err != nil {
		t.Error("Error should be nil.")
	}
}

func TestSynchronizeLargeSegments(t *testing.T) {
	updater := NewNoOpLargeSegmentUpdater()

	err := updater.SynchronizeLargeSegments()
	if err != nil {
		t.Error("Error should be nil.")
	}
}

func TestIsCached(t *testing.T) {
	updater := NewNoOpLargeSegmentUpdater()

	cached := updater.IsCached("name")
	if cached {
		t.Error("cached should be false.")
	}
}
