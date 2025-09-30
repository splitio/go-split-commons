package datatypes

import (
	"encoding/csv"
	"io"
	"os"
	"testing"
)

type Semvers struct {
	semver1 string
	semver2 string
	semver3 string
}

func TestCompareSemverToGreaterAndEqual(t *testing.T) {
	semvers, err := parseCSVTwoSemvers("../../../testdata/valid_semantic_versions.csv")
	if err != nil {
		t.Error(err)
	}

	for _, semversPair := range semvers {
		semver1, err := BuildSemver(semversPair.semver1)
		if err != nil {
			t.Error("should create semver1")
		}
		semver2, err := BuildSemver(semversPair.semver2)
		if err != nil {
			t.Error("should create semver2")
		}

		if semver1.Compare(*semver2) < 0 {
			t.Error("semver 1 should be greather than semver 2")
		}
		if semver2.Compare(*semver1) > 0 {
			t.Error("semver 1 should be greather than semver 2")
		}
		if semver1.Compare(*semver1) != 0 {
			t.Error("semver 1 should be equal to semver 1")
		}
		if semver2.Compare(*semver2) != 0 {
			t.Error("semver 2 should be equal to semver 2")
		}
	}
}

func TestInvalidFormats(t *testing.T) {
	semvers, err := parseCSVOneSemver("../../../testdata/invalid_semantic_versions.csv")
	if err != nil {
		t.Error(err)
	}
	for _, semver := range semvers {
		_, err := BuildSemver(semver)
		if err == nil {
			t.Error("should not create semver")
		}
	}
}

func TestEqualTo(t *testing.T) {
	semvers, err := parseCSVTwoSemvers("../../../testdata/equal_to_semver.csv")
	if err != nil {
		t.Error(err)
	}

	for _, semversPair := range semvers {
		semver1, err := BuildSemver(semversPair.semver1)
		if err != nil {
			t.Error("should create semver1")
		}
		semver2, err := BuildSemver(semversPair.semver2)
		if err != nil {
			t.Error("should create semver2")
		}

		if semver1.Compare(*semver2) != 0 {
			t.Error("semver 1 should be equal to semver 2")
		}
	}
}

func TestBetween(t *testing.T) {
	semvers, err := parseCSVThreeSemvers("../../../testdata/between_semver.csv")
	if err != nil {
		t.Error(err)
	}

	for _, threeSemvers := range semvers {
		semver1, err := BuildSemver(threeSemvers.semver1)
		if err != nil {
			t.Error("should create semver1")
		}
		semver2, err := BuildSemver(threeSemvers.semver2)
		if err != nil {
			t.Error("should create semver2")
		}
		semver3, err := BuildSemver(threeSemvers.semver3)
		if err != nil {
			t.Error("should create semver2")
		}

		if semver2.Compare(*semver1) < 0 && semver2.Compare(*semver3) > 0 {
			t.Error("semver 2 should be between to semver 1 and semver 3")
		}
	}
}

func TestGetVersion(t *testing.T) {
	semver, err := BuildSemver("02.03.04-02.04")
	if err != nil {
		t.Error("should create semver1")
	}
	if semver.Version() != "2.3.4-2.4" {
		t.Error("semver should build 2.3.4-2.4")
	}
}

func parseCSVOneSemver(file string) ([]string, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	csvr := csv.NewReader(f)

	var results []string
	for {
		row, err := csvr.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return results, err
		}
		results = append(results, row[0])
	}
}

func parseCSVTwoSemvers(file string) ([]Semvers, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	csvr := csv.NewReader(f)

	var results []Semvers
	for {
		row, err := csvr.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return results, err
		}
		results = append(results, Semvers{
			semver1: row[0],
			semver2: row[1],
		})
	}
}

func parseCSVThreeSemvers(file string) ([]Semvers, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	csvr := csv.NewReader(f)

	var results []Semvers
	for {
		row, err := csvr.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return results, err
		}
		results = append(results, Semvers{
			semver1: row[0],
			semver2: row[1],
			semver3: row[2],
		})
	}
}
