package local

import (
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/splitio/go-split-commons/v8/dtos"
	"github.com/splitio/go-split-commons/v8/service"
	"github.com/splitio/go-toolkit/v5/logging"

	yaml "gopkg.in/yaml.v3"
)

const defaultTill = -1

// FileSplitFetcher struct fetches splits from a file
type FileSplitFetcher struct {
	reader     Reader
	splitFile  string
	fileFormat int
	lastHash   []byte
	lastHashRB []byte
	logger     logging.LoggerInterface
	mutexFF    sync.Mutex
	mutexRB    sync.Mutex
}

// NewFileSplitFetcher returns a new instance of LocalFileSplitFetcher
func NewFileSplitFetcher(splitFile string, logger logging.LoggerInterface, fileFormat int) service.SplitFetcher {
	return &FileSplitFetcher{
		splitFile:  splitFile,
		fileFormat: fileFormat,
		logger:     logger,
		reader:     NewFileReader(),
		mutexFF:    sync.Mutex{},
		mutexRB:    sync.Mutex{},
	}
}

func parseSplitsClassic(data string) []dtos.SplitDTO {
	splits := make([]dtos.SplitDTO, 0)
	lines := strings.Split(data, "\n")
	for _, line := range lines {
		words := strings.Fields(line)
		if len(words) < 2 || len(words[0]) < 1 || words[0][0] == '#' {
			// Skip the line if it has less than two words, the words are empty strings or
			// it begins with '#' character
			continue
		}
		splitName := words[0]
		treatment := words[1]
		splits = append(splits, createSplit(
			splitName,
			treatment,
			createRolloutCondition(treatment),
			make(map[string]string),
		))
	}
	return splits
}

func createSplit(splitName string, treatment string, condition dtos.ConditionDTO, configurations map[string]string) dtos.SplitDTO {
	split := dtos.SplitDTO{
		Name:              splitName,
		TrafficAllocation: 100,
		Conditions:        []dtos.ConditionDTO{condition},
		Status:            "ACTIVE",
		DefaultTreatment:  "control",
		Configurations:    configurations,
	}
	return split
}

func createWhitelistedCondition(treatment string, keys interface{}) dtos.ConditionDTO {
	var whitelist []string
	switch keys := keys.(type) {
	case string:
		whitelist = []string{keys}
	case []string:
		whitelist = keys
	case []interface{}:
		whitelist = make([]string, 0)
		for _, key := range keys {
			k, ok := key.(string)
			if ok {
				whitelist = append(whitelist, k)
			}
		}
	default:
		whitelist = make([]string, 0)
	}
	return dtos.ConditionDTO{
		ConditionType: "WHITELIST",
		Label:         "LOCAL_",
		MatcherGroup: dtos.MatcherGroupDTO{
			Combiner: "AND",
			Matchers: []dtos.MatcherDTO{
				{
					MatcherType: "WHITELIST",
					Negate:      false,
					Whitelist: &dtos.WhitelistMatcherDataDTO{
						Whitelist: whitelist,
					},
				},
			},
		},
		Partitions: []dtos.PartitionDTO{
			{
				Size:      100,
				Treatment: treatment,
			},
		},
	}
}

func createRolloutCondition(treatment string) dtos.ConditionDTO {
	return dtos.ConditionDTO{
		ConditionType: "ROLLOUT",
		Label:         "LOCAL_ROLLOUT",
		MatcherGroup: dtos.MatcherGroupDTO{
			Combiner: "AND",
			Matchers: []dtos.MatcherDTO{
				{
					MatcherType: "ALL_KEYS",
					Negate:      false,
				},
			},
		},
		Partitions: []dtos.PartitionDTO{
			{
				Size:      100,
				Treatment: treatment,
			},
			{
				Size:      0,
				Treatment: "_",
			},
		},
	}
}

func createCondition(keys interface{}, treatment string) dtos.ConditionDTO {
	if keys != nil {
		return createWhitelistedCondition(treatment, keys)
	}
	return createRolloutCondition(treatment)
}

func (f *FileSplitFetcher) parseSplitsYAML(data string) (d []dtos.SplitDTO) {
	// Set up a guard deferred function to recover if some error occurs during parsing
	defer func() {
		if r := recover(); r != nil {
			// At this point we'll only trust that the logger isn't panicking trust
			// that the logger isn't panicking
			f.logger.Error(fmt.Sprintf("Localhost Parsing: %v", string(debug.Stack())))
			d = make([]dtos.SplitDTO, 0)
		}
	}()

	splits := make([]dtos.SplitDTO, 0)

	var splitsFromYAML []map[string]map[string]interface{}
	err := yaml.Unmarshal([]byte(data), &splitsFromYAML)
	if err != nil {
		f.logger.Error(fmt.Sprintf("error: %v", err))
		return splits
	}

	splitsToParse := make(map[string]dtos.SplitDTO, 0)

	for _, splitMap := range splitsFromYAML {
		for splitName, splitParsed := range splitMap {
			split, ok := splitsToParse[splitName]
			treatment, isString := splitParsed["treatment"].(string)
			if !isString {
				break
			}
			config, isValidConfig := splitParsed["config"].(string)
			if !ok {
				configurations := make(map[string]string)
				if isValidConfig {
					configurations[treatment] = config
				}
				splitsToParse[splitName] = createSplit(
					splitName,
					treatment,
					createCondition(splitParsed["keys"], treatment),
					configurations,
				)
			} else {
				newCondition := createCondition(splitParsed["keys"], treatment)
				if newCondition.ConditionType == "ROLLOUT" {
					split.Conditions = append(split.Conditions, newCondition)
				} else {
					split.Conditions = append([]dtos.ConditionDTO{newCondition}, split.Conditions...)
				}
				configurations := split.Configurations
				if isValidConfig {
					configurations[treatment] = config
				}
				split.Configurations = configurations
				splitsToParse[splitName] = split
			}

		}
	}

	for _, split := range splitsToParse {
		splits = append(splits, split)
	}

	return splits
}

func (f *FileSplitFetcher) parseSplitsJson(data string) (*dtos.FFResponseLocalV13, error) {
	splitChangesDto, err := dtos.NewFFResponseLocalV13([]byte(data))

	if err != nil {
		f.logger.Error(fmt.Sprintf("error: %v", err))
		return nil, fmt.Errorf("couldn't parse splitChange json")
	}
	return splitSanitization(splitChangesDto), nil
}

func (s *FileSplitFetcher) processSplitJson(data string, changeNumber int64) (dtos.FFResponse, error) {
	splitChange, err := s.parseSplitsJson(data)
	if err != nil {
		s.logger.Error(fmt.Sprintf("could not find the splitChange file. error: %v", err))
		return nil, err
	}
	// if the till is less than storage CN and different from the default till ignore the change
	if splitChange.FFTill() < changeNumber && splitChange.FFTill() != defaultTill ||
		splitChange.RBTill() != 0 && splitChange.RBTill() < changeNumber && splitChange.RBTill() != defaultTill {
		return nil, fmt.Errorf("ignoring change, the till is less than storage change number")
	}
	splitsJson, _ := json.Marshal(splitChange.FeatureFlags())
	currH := sha1.New()
	currH.Write(splitsJson)
	// calculate the json sha
	currSum := currH.Sum(nil)
	s.mutexFF.Lock()
	defer s.mutexFF.Unlock()
	//if sha exist and is equal to before sha, or if till is equal to default till returns the same splitChange with till equals to storage CN
	if bytes.Equal(currSum, s.lastHash) || splitChange.FFTill() == defaultTill {
		s.lastHash = currSum
		splitChange.SetFFTill(changeNumber)
		splitChange.SetFFSince(changeNumber)
		return splitChange, nil
	}
	// In the last case, the sha is different and till upper or equal to storage CN
	s.lastHash = currSum

	if splitChange.RuleBasedSegments() != nil {
		ruleBasedJson, _ := json.Marshal(splitChange.RuleBasedSegments())
		currHRB := sha1.New()
		currHRB.Write(ruleBasedJson)
		// calculate the json sha
		currSumRB := currHRB.Sum(nil)
		s.mutexRB.Lock()
		defer s.mutexRB.Unlock()
		//if sha exist and is equal to before sha, or if till is equal to default till returns the same splitChange with till equals to storage CN
		if bytes.Equal(currSumRB, s.lastHashRB) || splitChange.RBTill() == defaultTill {
			s.lastHashRB = currSumRB
			splitChange.SetRBTill(changeNumber)
			splitChange.SetRBSince(changeNumber)
			return splitChange, nil
		}
		s.lastHashRB = currSumRB
		splitChange.SetRBSince(splitChange.RBTill())
	}

	// In the last case, the sha is different and till upper or equal to storage CN
	s.lastHash = currSum
	splitChange.SetFFSince(splitChange.FFTill())
	return splitChange, nil
}

// Fetch parses the file and returns the appropriate structures
func (s *FileSplitFetcher) Fetch(fetchOptions *service.FlagRequestParams) (dtos.FFResponse, error) {
	fileContents, err := s.reader.ReadFile(s.splitFile)
	if err != nil {
		return nil, err
	}

	var splits []dtos.SplitDTO
	data := string(fileContents)
	switch s.fileFormat {
	case SplitFileFormatClassic:
		splits = parseSplitsClassic(data)
	case SplitFileFormatYAML:
		splits = s.parseSplitsYAML(data)
	case SplitFileFormatJSON:
		return s.processSplitJson(data, fetchOptions.ChangeNumber())
	default:
		return nil, fmt.Errorf("unsupported file format")

	}

	till := fetchOptions.ChangeNumber()

	// Get the SHA1 sum of the raw contents of the file, and compare it to the last one seen
	// if it's equal, nothing has changed, return since == till
	// otherwise, something changed, return till = since + 1
	currH := sha1.New()
	currH.Write(fileContents)
	currSum := currH.Sum(nil)
	if !bytes.Equal(currSum, s.lastHash) {
		till++
	}

	s.lastHash = currSum
	return &dtos.FFResponseV13{
		SplitChanges: dtos.SplitChangesDTO{
			FeatureFlags: dtos.FeatureFlagsDTO{
				Splits: splits,
				Since:  fetchOptions.ChangeNumber(),
				Till:   till,
			},
		},
	}, nil
}

func (s *FileSplitFetcher) IsProxy(fetchOptions *service.FlagRequestParams) bool {
	return false
}

var _ service.SplitFetcher = &FileSplitFetcher{}
