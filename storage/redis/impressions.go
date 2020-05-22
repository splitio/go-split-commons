package redis

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-toolkit/logging"
	"github.com/splitio/go-toolkit/redis"
	"github.com/splitio/split-synchronizer/log"
)

const impressionsTTLRefresh = time.Duration(3600) * time.Second

// ImpressionStorage is a redis-based implementation of split storage
type ImpressionStorage struct {
	client          *redis.PrefixedRedisClient
	mutex           *sync.Mutex
	logger          logging.LoggerInterface
	redisKey        string
	impressionsTTL  time.Duration
	metadataMessage dtos.Metadata
}

// NewImpressionStorage creates a new RedisSplitStorage and returns a reference to it
func NewImpressionStorage(client *redis.PrefixedRedisClient, metadata dtos.Metadata, logger logging.LoggerInterface) *ImpressionStorage {
	return &ImpressionStorage{
		client:          client,
		mutex:           &sync.Mutex{},
		logger:          logger,
		redisKey:        redisImpressionsQueue,
		impressionsTTL:  redisImpressionsTTL,
		metadataMessage: metadata,
	}
}

// LogImpressions stores impressions in redis as Queue
func (r *ImpressionStorage) LogImpressions(impressions []dtos.Impression) error {
	var impressionsToStore []dtos.ImpressionQueueObject
	for _, i := range impressions {
		var impression = dtos.ImpressionQueueObject{Metadata: r.metadataMessage, Impression: i}
		impressionsToStore = append(impressionsToStore, impression)
	}

	if len(impressionsToStore) > 0 {
		return r.push(impressionsToStore)
	}
	return nil
}

// push stores impressions in redis
func (r *ImpressionStorage) push(impressions []dtos.ImpressionQueueObject) error {
	var impressionsJSON []interface{}
	for _, impression := range impressions {
		iJSON, err := json.Marshal(impression)
		if err != nil {
			r.logger.Error("Error encoding impression in json")
			r.logger.Error(err)
		} else {
			impressionsJSON = append(impressionsJSON, iJSON)
		}
	}

	r.logger.Debug("Pushing impressions to: ", r.redisKey, len(impressionsJSON))

	inserted, errPush := r.client.RPush(r.redisKey, impressionsJSON...)
	if errPush != nil {
		r.logger.Error("Something were wrong pushing impressions to redis", errPush)
		return errPush
	}

	// Checks if expiration needs to be set
	if inserted == int64(len(impressionsJSON)) {
		r.logger.Debug("Proceeding to set expiration for: ", r.redisKey)
		result := r.client.Expire(r.redisKey, time.Duration(r.impressionsTTL)*time.Minute)
		if result == false {
			r.logger.Error("Something were wrong setting expiration", errPush)
		}
	}
	return nil
}

// PopN return N elements from 0 to N
func (r *ImpressionStorage) PopN(n int64) ([]dtos.Impression, error) {
	toReturn := make([]dtos.Impression, 0, 0)

	/*
		lrange, err := r.fetchImpressionsFromQueueWithLock(n)
		if err != nil {
			return nil, err
		}

		//JSON unmarshal
		for _, se := range lrange {
			storedImpression := dtos.ImpressionQueueObject{}
			err := json.Unmarshal([]byte(se), &storedImpression)
			if err != nil {
				r.logger.Error("Error decoding impression JSON", err.Error())
				continue
			}
			if storedImpression.Metadata.MachineIP == r.metadataMessage.MachineIP &&
				storedImpression.Metadata.MachineName == r.metadataMessage.MachineName &&
				storedImpression.Metadata.SDKVersion == r.metadataMessage.SDKVersion {
				toReturn = append(toReturn, storedImpression.Impression)
			}
		}
	*/

	return toReturn, nil
}

func (r *ImpressionStorage) fetchImpressionsFromQueueWithLock(n int64) ([]string, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	lrange, err := r.client.LRange(r.redisKey, 0, n-1)
	if err != nil {
		r.logger.Error("Fetching impressions", err.Error())
		r.mutex.Unlock()
		return nil, err
	}
	totalFetchedImpressions := int64(len(lrange))

	idxFrom := n
	if totalFetchedImpressions < n {
		idxFrom = totalFetchedImpressions
	}

	err = r.client.LTrim(r.redisKey, idxFrom, -1)
	if err != nil {
		r.logger.Error("Trim impressions", err.Error())
		r.mutex.Unlock()
		return nil, err
	}

	// This operation will simply do nothing if the key no longer exists (queue is empty)
	// It's only done in the "successful" exit path so that the TTL is not overriden if impressons weren't
	// popped correctly. This will result in impressions getting lost but will prevent the queue from taking
	// a huge amount of memory.
	r.client.Expire(r.redisKey, impressionsTTLRefresh)
	return lrange, nil
}

func toImpressionsDTO(impressionsMap map[string][]dtos.ImpressionDTO) ([]dtos.ImpressionsDTO, error) {
	if impressionsMap == nil {
		return nil, fmt.Errorf("Impressions map cannot be null")
	}

	toReturn := make([]dtos.ImpressionsDTO, 0)
	for feature, impressions := range impressionsMap {
		toReturn = append(toReturn, dtos.ImpressionsDTO{
			TestName:       feature,
			KeyImpressions: impressions,
		})
	}
	return toReturn, nil
}

// fetchImpressionsFromQueue retrieves impressions from a redis list acting as a queue.
func (r *ImpressionStorage) fetchImpressionsFromQueue(count int64) (map[dtos.Metadata][]dtos.ImpressionsDTO, error) {
	impressionsRawList, err := r.fetchImpressionsFromQueueWithLock(count)
	if err != nil {
		return nil, err
	}

	// grouping the information by instanceID/instanceIP, and then by feature name
	collectedData := make(map[dtos.Metadata]map[string][]dtos.ImpressionDTO)

	for _, rawImpression := range impressionsRawList {
		var impression dtos.ImpressionQueueObject
		err := json.Unmarshal([]byte(rawImpression), &impression)
		if err != nil {
			log.Error.Println("Error decoding impression JSON", err.Error())
			continue
		}

		_, instanceExists := collectedData[impression.Metadata]
		if !instanceExists {
			collectedData[impression.Metadata] = make(map[string][]dtos.ImpressionDTO)
		}

		_, featureExists := collectedData[impression.Metadata][impression.Impression.FeatureName]
		if !featureExists {
			collectedData[impression.Metadata][impression.Impression.FeatureName] = make([]dtos.ImpressionDTO, 0)
		}

		collectedData[impression.Metadata][impression.Impression.FeatureName] = append(
			collectedData[impression.Metadata][impression.Impression.FeatureName],
			dtos.ImpressionDTO{
				BucketingKey: impression.Impression.BucketingKey,
				ChangeNumber: impression.Impression.ChangeNumber,
				KeyName:      impression.Impression.KeyName,
				Label:        impression.Impression.Label,
				Time:         impression.Impression.Time,
				Treatment:    impression.Impression.Treatment,
			},
		)
	}

	toReturn := make(map[dtos.Metadata][]dtos.ImpressionsDTO)
	for metadata, impsForMetadata := range collectedData {
		toReturn[metadata], err = toImpressionsDTO(impsForMetadata)
		if err != nil {
			log.Error.Printf("Unable to write impressions for metadata %v", metadata)
			continue
		}
	}

	return toReturn, nil
}

// RetrieveImpressions returns impressions stored in redis
func (r *ImpressionStorage) RetrieveImpressions(count int64) (map[dtos.Metadata][]dtos.ImpressionsDTO, error) {
	impressions, err := r.fetchImpressionsFromQueue(count)
	if err != nil {
		return nil, err
	}

	return impressions, nil
}

// Size returns the size of the impressions queue
func (r *ImpressionStorage) Size() int64 {
	val, err := r.client.LLen(r.redisKey)
	if err != nil {
		return 0
	}
	return val
}

// Empty returns true if redis list is zero length
func (r *ImpressionStorage) Empty() bool {
	return r.Size() == 0
}
