package local

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/splitio/go-toolkit/v5/logging"
)

func TestLocalSplitFetcher(t *testing.T) {
	file, err := ioutil.TempFile("", "localhost_test")
	if err != nil {
		t.Error("should no fail to open temp file. Got: ", err)
	}
	defer os.Remove(file.Name())

	if _, err := file.Write([]byte("feature1 on\nfeature2 off\n")); err != nil {
		t.Error("writing to the file should not fail")
	}

	if err := file.Sync(); err != nil {
		t.Error("syncing the file should not fail")
	}

	logger := logging.NewLogger(nil)
	fetcher := NewFileSplitFetcher(file.Name(), logger)

	res, err := fetcher.Fetch(-1, true)
	if err != nil {
		t.Error("fetching should not fail. Got: ", err)
	}

	if res.Since != -1 || res.Till != 0 {
		t.Error("Wrong since/till. Got: ", res.Since, res.Till)
	}

	if len(res.Splits) != 2 {
		t.Error("should have 2 splits. has: ", res.Splits)
	}

	// second call -- no change -- since == till

	res, err = fetcher.Fetch(0, true)
	if err != nil {
		t.Error("fetching should not fail. Got: ", err)
	}

	if res.Since != 0 || res.Till != 0 {
		t.Error("Wrong since/till. Got: ", res.Since, res.Till)
	}

	if len(res.Splits) != 2 {
		t.Error("should have 2 splits. has: ", res.Splits)
	}

	if _, err := file.Write([]byte("feature3 yes\n")); err != nil {
		t.Error("writing to the file should not fail")
	}

	// third call -- change -- till > since

	res, err = fetcher.Fetch(0, true)
	if err != nil {
		t.Error("fetching should not fail. Got: ", err)
	}

	if res.Since != 0 || res.Till != 1 {
		t.Error("Wrong since/till. Got: ", res.Since, res.Till)
	}

	if len(res.Splits) != 3 {
		t.Error("should have 2 splits. has: ", res.Splits)
	}

	// fourth call -- no change -- till != since

	res, err = fetcher.Fetch(1, true)
	if err != nil {
		t.Error("fetching should not fail. Got: ", err)
	}

	if res.Since != 1 || res.Till != 1 {
		t.Error("Wrong since/till. Got: ", res.Since, res.Till)
	}

	if len(res.Splits) != 3 {
		t.Error("should have 2 splits. has: ", res.Splits)
	}

}
