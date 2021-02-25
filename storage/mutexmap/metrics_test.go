package mutexmap

import (
	"reflect"
	"testing"

	"github.com/splitio/go-split-commons/v3/dtos"
)

func indexOf(array interface{}, callback func(item interface{}) bool) (int, bool) {
	switch reflect.TypeOf(array).Kind() {
	case reflect.Slice:
		castedArray := reflect.ValueOf(array)
		for i := 0; i < castedArray.Len(); i++ {
			if callback(castedArray.Index(i).Interface()) {
				return i, true
			}
		}
	}
	return 0, false
}

func TestMetricsStorage(t *testing.T) {
	metricsStorage := NewMMMetricsStorage()

	// Gauges
	metricsStorage.PutGauge("gauge1", 123.123)
	metricsStorage.PutGauge("gauge2", 456.456)
	metricsStorage.PutGauge("gauge3", 789.789)

	if len(metricsStorage.gaugeData) != 3 {
		t.Error("Incorrect number of gauges in storage")
	}

	gaugesBak := metricsStorage.gaugeData
	gauges := metricsStorage.PopGauges()

	if len(gauges) != 3 {
		t.Error("Incorrect number of gauges popped")
	}

	for key := range gaugesBak {
		index, found := indexOf(gauges, func(i interface{}) bool {
			orig, ok := i.(dtos.GaugeDTO)
			if ok && orig.MetricName == key {
				return true
			}
			return false
		})
		if !found {
			t.Errorf("Gauge %s should be present in storage and is not.", key)
		} else {
			if gauges[index].Gauge != gaugesBak[key] {
				t.Errorf("Value for gauge %s is incorrect", key)
			}
		}
	}

	metricsStorage.IncCounter("counter1")
	metricsStorage.IncCounter("counter1")
	metricsStorage.IncCounter("counter1")
	metricsStorage.IncCounter("counter2")

	if len(metricsStorage.counterData) != 2 {
		t.Error("Incorrect number of counters in storage")
	}

	countersBak := metricsStorage.counterData
	counters := metricsStorage.PopCounters()
	if len(counters) != 2 {
		t.Error("Incorrect number of counters popped")
	}

	for key := range countersBak {
		index, found := indexOf(counters, func(i interface{}) bool {
			orig, ok := i.(dtos.CounterDTO)
			if ok && orig.MetricName == key {
				return true
			}
			return false
		})
		if !found {
			t.Errorf("Counter %s should be present in storage and is not.", key)
		} else {
			if counters[index].Count != countersBak[key] {
				t.Errorf("Value for counter %s is incorrect", key)
			}
		}
	}

	metricsStorage.IncLatency("http_io", 1)
	metricsStorage.IncLatency("http_io", 1)
	metricsStorage.IncLatency("http_io", 1)
	metricsStorage.IncLatency("http_io", 4)
	metricsStorage.IncLatency("disk_io", 7)

	if len(metricsStorage.latenciesData) != 2 {
		t.Error("Incorrect number of latencies in storage")
	}

	latenciesBak := metricsStorage.latenciesData
	latencies := metricsStorage.PopLatencies()
	if len(latencies) != 2 {
		t.Error("Incorrect number of latencies popped")
	}

	for key := range latenciesBak {
		index, found := indexOf(latencies, func(i interface{}) bool {
			orig, ok := i.(dtos.LatenciesDTO)
			if ok && orig.MetricName == key {
				return true
			}
			return false
		})
		if !found {
			t.Errorf("Counter %s should be present in storage and is not.", key)
		} else {
			eq := true
			for li := range latenciesBak[key] {
				if latencies[index].Latencies[li] != latenciesBak[key][li] {
					eq = false
				}
			}
			if !eq {
				t.Errorf("Value for counter %s is incorrect", key)
			}
		}
	}
}
