package redis

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/splitio/go-split-commons/dtos"
	"github.com/splitio/go-toolkit/logging"
	"github.com/splitio/go-toolkit/redis"
	"github.com/splitio/split-synchronizer/log"
)

// MetricsStorage is a redis-based implementation of split storage
type MetricsStorage struct {
	client            redis.PrefixedRedisClient
	logger            logging.LoggerInterface
	gaugeTemplate     string
	countersTemplate  string
	latenciesTemplate string
	latenciesRegexp   *regexp.Regexp
}

// NewMetricsStorage creates a new RedisSplitStorage and returns a reference to it
func NewMetricsStorage(redisClient *redis.PrefixedRedisClient, metadata dtos.Metadata, logger logging.LoggerInterface) *MetricsStorage {
	gaugeTemplate := strings.Replace(redisGauge, "{sdkVersion}", metadata.SDKVersion, 1)
	gaugeTemplate = strings.Replace(gaugeTemplate, "{instanceId}", metadata.MachineName, 1)
	countersTemplate := strings.Replace(redisCount, "{sdkVersion}", metadata.SDKVersion, 1)
	countersTemplate = strings.Replace(countersTemplate, "{instanceId}", metadata.MachineName, 1)
	latenciesTemplate := strings.Replace(redisLatency, "{sdkVersion}", metadata.SDKVersion, 1)
	latenciesTemplate = strings.Replace(latenciesTemplate, "{instanceId}", metadata.MachineName, 1)
	latencyRegex := regexp.MustCompile(redisLatencyRegex)
	return &MetricsStorage{
		client:            *redisClient,
		logger:            logger,
		gaugeTemplate:     gaugeTemplate,
		countersTemplate:  countersTemplate,
		latenciesTemplate: latenciesTemplate,
		latenciesRegexp:   latencyRegex,
	}
}

// PutGauge stores a gauge in redis
func (r *MetricsStorage) PutGauge(key string, gauge float64) {
	keyToStore := strings.Replace(r.gaugeTemplate, "{metric}", key, 1)
	err := r.client.Set(keyToStore, gauge, 0)
	if err != nil {
		r.logger.Error(fmt.Sprintf("Error storing gauge \"%s\" in redis: %s\n", key, err))
	}
}

// IncLatency incraeses the latency of a bucket for a specific metric
func (r *MetricsStorage) IncLatency(metric string, index int) {
	keyToIncr := strings.Replace(r.latenciesTemplate, "{metric}", metric, 1)
	keyToIncr = strings.Replace(keyToIncr, "{bucket}", strconv.FormatInt(int64(index), 10), 1)
	_, err := r.client.Incr(keyToIncr)
	if err != nil {
		r.logger.Error(fmt.Sprintf(
			"Error incrementing latency bucket %d for metric \"%s\" in redis: %s", index, metric, err.Error(),
		))
	}
}

// IncCounter incraeses the count for a specific metric
func (r *MetricsStorage) IncCounter(metric string) {
	keyToIncr := strings.Replace(r.countersTemplate, "{metric}", metric, 1)
	_, err := r.client.Incr(keyToIncr)
	if err != nil {
		r.logger.Error(fmt.Sprintf("Error incrementing counterfor metric \"%s\" in redis: %s", metric, err.Error()))
	}
}

// GaugeDataBulk metrics
type GaugeDataBulk struct {
	data map[string]map[string]map[string]float64
}

// PutGauge adds a gauge to the structure
func (l *GaugeDataBulk) PutGauge(sdk string, machineIP string, metricName string, value float64) {
	if _, ok := l.data[sdk]; !ok {
		l.data[sdk] = make(map[string]map[string]float64)
	}

	if _, ok := l.data[sdk][machineIP]; !ok {
		l.data[sdk][machineIP] = make(map[string]float64)
	}

	l.data[sdk][machineIP][metricName] = value
}

func (r *MetricsStorage) popByPattern(pattern string, useTransaction bool) (map[string]interface{}, error) {
	keys, err := r.client.Keys(pattern)
	if err != nil {
		r.logger.Error(err.Error())
		return nil, err
	}

	if len(keys) == 0 {
		return map[string]interface{}{}, nil
	}

	values, err := r.client.MGet(keys)
	if err != nil {
		r.logger.Error(err.Error())
		return nil, err
	}
	_, err = r.client.Del(keys...)
	if err != nil {
		// if we failed to delete the keys, log an error and continue working.
		r.logger.Error(err.Error())
	}

	toReturn := make(map[string]interface{})
	for index := range keys {
		if index >= len(keys) || index >= len(values) {
			break
		}
		toReturn[keys[index]] = values[index]
	}
	return toReturn, nil

}

func parseMetricKey(metricType string, key string) (string, string, string, error) {
	var re = regexp.MustCompile(strings.Replace(
		`(\w+.)?SPLITIO\/([^\/]+)\/([^\/]+)\/{metricType}.([\s\S]*)`,
		"{metricType}",
		metricType,
		1,
	))
	match := re.FindStringSubmatch(key)

	if len(match) < 5 {
		return "", "", "", fmt.Errorf("Error parsing key %s", key)
	}

	sdkNameAndVersion := match[2]
	if sdkNameAndVersion == "" {
		return "", "", "", fmt.Errorf("Invalid sdk name/version")
	}

	machineIP := match[3]
	if machineIP == "" {
		return "", "", "", fmt.Errorf("Invalid machine IP")
	}

	metricName := match[4]
	if metricName == "" {
		return "", "", "", fmt.Errorf("Invalid feature name")
	}

	log.Verbose.Println("Impression parsed key", match)

	return sdkNameAndVersion, machineIP, metricName, nil
}

func parseFloatRedisValue(s interface{}) (float64, error) {
	asStr, ok := s.(string)
	if !ok {
		return 0, fmt.Errorf("%+v is not a string", s)
	}

	asFloat64, err := strconv.ParseFloat(asStr, 64)
	if err != nil {
		return 0, err
	}

	return asFloat64, nil
}

// RetrieveGauges returns gauges values saved in Redis by SDKs
func (r *MetricsStorage) RetrieveGauges() (*GaugeDataBulk, error) {
	pattern := strings.Replace(redisGauge, "{sdkVersion}", "*", 1)
	pattern = strings.Replace(pattern, "{instanceId}", "*", 1)
	pattern = strings.Replace(pattern, "{metric}", "*", 1)
	const _metricsGaugesNamespace = "SPLITIO/*/*/gauge.*"
	data, err := r.popByPattern(pattern, false)
	if err != nil {
		log.Error.Println(err.Error())
		return nil, err
	}

	gaugesToReturn := &GaugeDataBulk{
		data: make(map[string]map[string]map[string]float64),
	}
	for key, value := range data {
		sdkNameAndVersion, machineIP, metricName, err := parseMetricKey("gauge", key)
		if err != nil {
			log.Error.Printf("Unable to parse key %s. Skipping", key)
			continue
		}
		asFloat, err := parseFloatRedisValue(value)
		if err != nil {
			log.Error.Printf("Unable to parse value %+v. Skipping", value)
			continue
		}
		gaugesToReturn.PutGauge(sdkNameAndVersion, machineIP, metricName, asFloat)
	}

	return gaugesToReturn, nil
}

// CounterDataBulk holds all counters fetched from storage sorted properly.
type CounterDataBulk struct {
	data map[string]map[string]map[string]int64
}

// PutCounter adds a counter to the structure
func (l *CounterDataBulk) PutCounter(sdk string, machineIP string, metricName string, value int64) {

	if _, ok := l.data[sdk]; !ok {
		l.data[sdk] = make(map[string]map[string]int64)
	}

	if _, ok := l.data[sdk][machineIP]; !ok {
		l.data[sdk][machineIP] = make(map[string]int64)
	}

	l.data[sdk][machineIP][metricName] = value
}

func parseIntRedisValue(s interface{}) (int64, error) {
	asStr, ok := s.(string)
	if !ok {
		return 0, fmt.Errorf("%+v is not a string", s)
	}

	asInt64, err := strconv.ParseInt(asStr, 10, 64)
	if err != nil {
		return 0, err
	}

	return asInt64, nil
}

// RetrieveCounters returns counter values saved in Redis by SDKs
func (r *MetricsStorage) RetrieveCounters() (*CounterDataBulk, error) {
	pattern := strings.Replace(redisCount, "{sdkVersion}", "*", 1)
	pattern = strings.Replace(pattern, "{instanceId}", "*", 1)
	pattern = strings.Replace(pattern, "{metric}", "*", 1)
	data, err := r.popByPattern(pattern, false)
	if err != nil {
		log.Error.Println(err.Error())
		return nil, err
	}

	countersToReturn := &CounterDataBulk{
		data: make(map[string]map[string]map[string]int64),
	}
	for key, value := range data {
		sdkNameAndVersion, machineIP, metricName, err := parseMetricKey("count", key)
		if err != nil {
			log.Error.Printf("Unable to parse key %s. Skipping", key)
			continue
		}
		asInt, err := parseIntRedisValue(value)
		if err != nil {
			log.Error.Println(err.Error())
			continue
		}

		countersToReturn.PutCounter(sdkNameAndVersion, machineIP, metricName, asInt)
	}

	return countersToReturn, nil
}

const maxBuckets = 23

// LatencyDataBulk holds all latencies fetched from storage sorted properly.
type LatencyDataBulk struct {
	data map[string]map[string]map[string][]int64
}

// PutLatency adds a new latency to the structure
func (l *LatencyDataBulk) PutLatency(sdk string, machineIP string, metricName string, bucketNumber int, value int64) {

	if _, ok := l.data[sdk]; !ok {
		l.data[sdk] = make(map[string]map[string][]int64)
	}

	if _, ok := l.data[sdk][machineIP]; !ok {
		l.data[sdk][machineIP] = make(map[string][]int64)
	}

	if _, ok := l.data[sdk][machineIP][metricName]; !ok {
		l.data[sdk][machineIP][metricName] = make([]int64, maxBuckets)
	}

	l.data[sdk][machineIP][metricName][bucketNumber] = value
}

func parseLatencyKey(key string) (string, string, string, int, error) {
	re := regexp.MustCompile(`(\w+.)?SPLITIO\/([^\/]+)\/([^\/]+)\/latency.([^\/]+).bucket.([0-9]*)`)
	match := re.FindStringSubmatch(key)

	if len(match) < 6 {
		return "", "", "", 0, fmt.Errorf("Error parsing key %s", key)
	}

	sdkNameAndVersion := match[2]
	if sdkNameAndVersion == "" {
		return "", "", "", 0, fmt.Errorf("Invalid sdk name/version")
	}

	machineIP := match[3]
	if machineIP == "" {
		return "", "", "", 0, fmt.Errorf("Invalid machine IP")
	}

	metricName := match[4]
	if metricName == "" {
		return "", "", "", 0, fmt.Errorf("Invalid feature name")
	}

	bucketNumber, err := strconv.Atoi(match[5])
	if err != nil {
		return "", "", "", 0, fmt.Errorf("Error parsing bucket number: %s", err.Error())
	}
	log.Verbose.Println("Impression parsed key", match)

	return sdkNameAndVersion, machineIP, metricName, bucketNumber, nil
}

// RetrieveLatencies returns latency values saved in Redis by SDKs
func (r *MetricsStorage) RetrieveLatencies() (*LatencyDataBulk, error) {
	//(\w+.)?SPLITIO\/([^\/]+)\/([^\/]+)\/latency.([^\/]+).bucket.([0-9]*)
	pattern := strings.Replace(redisLatency, "{sdkVersion}", "*", 1)
	pattern = strings.Replace(pattern, "{instanceId}", "*", 1)
	pattern = strings.Replace(pattern, "{metric}", "*", 1)
	data, err := r.popByPattern(pattern, false)
	if err != nil {
		log.Error.Println(err.Error())
		return nil, err
	}

	latenciesToReturn := &LatencyDataBulk{
		data: make(map[string]map[string]map[string][]int64),
	}
	for key, value := range data {
		value, err := parseIntRedisValue(value)
		if err != nil {
			log.Warning.Printf("Unable to parse value of key %s. Skipping", key)
			continue
		}
		sdkNameAndVersion, machineIP, metricName, bucketNumber, err := parseLatencyKey(key)
		if err != nil {
			log.Warning.Printf("Unable to parse key %s. Skipping", key)
			continue
		}
		latenciesToReturn.PutLatency(sdkNameAndVersion, machineIP, metricName, bucketNumber, value)
	}
	log.Verbose.Println(latenciesToReturn)
	return latenciesToReturn, nil
}
