package collector

import (
	"encoding/json"
	"fmt"
	"fullerite/metric"
	"reflect"
	"testing"
	"time"
	"io/ioutil"


	l "github.com/Sirupsen/logrus"
	"github.com/docker/engine-api/client"
	"github.com/docker/engine-api/types"
	"github.com/stretchr/testify/assert"
)

func contains(t *testing.T, metrics []metric.Metric, other metric.Metric) bool {
	mdone := false
	for _, my := range metrics {
		if my.Name == other.Name {
			fmt.Printf("%s => my.Buffered:%v | other.Buffered:%v\n", my.Name, my.Buffered, other.Buffered)
			assert.Equal(t, my.MetricType, other.MetricType)
			assert.Equal(t, my.Value, other.Value)
			assert.Equal(t, my.Dimensions, other.Dimensions)
			assert.Equal(t, my.GetTime(), other.GetTime())
			assert.Equal(t, my.Buffered, other.Buffered)
			mdone = true
		}
	}
	if !mdone {
		assert.True(t, false, fmt.Sprintf("%s not found in metrics", other.Name))
	}
	return true
}

func getSUT() *DockerStats {
	expectedChan := make(chan metric.Metric)
	var expectedLogger = defaultLog.WithFields(l.Fields{"collector": "fullerite"})

	return newDockerStats(expectedChan, 10, expectedLogger).(*DockerStats)
}

func TestDockerStatsNewDockerStats(t *testing.T) {
	expectedChan := make(chan metric.Metric)
	var expectedLogger = defaultLog.WithFields(l.Fields{"collector": "fullerite"})
	expectedType := make(map[string]*CPUValues)

	d := newDockerStats(expectedChan, 10, expectedLogger).(*DockerStats)

	assert.Equal(t, d.log, expectedLogger)
	assert.Equal(t, d.channel, expectedChan)
	assert.Equal(t, d.interval, 10)
	assert.Equal(t, d.name, "DockerStats")
	assert.Equal(t, reflect.TypeOf(d.previousCPUValues), reflect.TypeOf(expectedType))
	assert.Equal(t, len(d.previousCPUValues), 0)
	d.Configure(make(map[string]interface{}))
	assert.Equal(t, d.GetEndpoint(), endpoint)
}

func TestDockerStatsConfigureEmptyConfig(t *testing.T) {
	config := make(map[string]interface{})

	d := newDockerStats(nil, 123, nil).(*DockerStats)
	d.Configure(config)

	assert.Equal(t, 123, d.Interval())
}

func TestDockerStatsConfigure(t *testing.T) {
	config := make(map[string]interface{})
	config["interval"] = 9999

	d := newDockerStats(nil, 123, nil).(*DockerStats)
	d.Configure(config)

	assert.Equal(t, 9999, d.Interval())
}

func TestDockerStatsBuildMetrics(t *testing.T) {
	config := make(map[string]interface{})
	envVars := []byte(`
	{
		"service_name":  {
			"MESOS_TASK_ID": "[^\\.]*"
		},
		"instance_name": {
			"MESOS_TASK_ID": "\\.([^\\.]*)\\."}
	}`)
	var val map[string]interface{}

	err := json.Unmarshal(envVars, &val)
	assert.Equal(t, err, nil)
	config["generatedDimensions"] = val

	stats := new(docker.Stats)
	stats.Networks = make(map[string]docker.NetworkStats)
	stats.Networks["eth0"] = docker.NetworkStats{RxBytes: 10, TxBytes: 20}
	stats.MemoryStats.Usage = 50
	stats.MemoryStats.Limit = 70

	containerJSON := []byte(`
	{
		"ID": "test-id",
		"Name": "test-container",
		"Config": {
			"Env": [
				"MESOS_TASK_ID=my--service.main.blablagit6bdsadnoise"
			]
		}
	}`)
	var container *docker.Container
	err = json.Unmarshal(containerJSON, &container)
	assert.Equal(t, err, nil)

	baseDims := map[string]string{
		"container_id":   "test-id",
		"container_name": "test-container",
		"service_name":   "my_service",
		"instance_name":  "main",
	}
	netDims := map[string]string{
		"container_id":   "test-id",
		"container_name": "test-container",
		"service_name":   "my_service",
		"instance_name":  "main",
		"iface":          "eth0",
	}

	expectedDimsGen := map[string]string{
		"service_name":  "my_service",
		"instance_name": "main",
	}
	now := time.Now()
	expectedMetrics := []metric.Metric{
		metric.NewExt("DockerRxBytes", "cumcounter", 10, netDims, now, false),
		metric.NewExt("DockerTxBytes", "cumcounter", 20, netDims, now, false),
		metric.NewExt("DockerMemoryUsed", "gauge", 50, baseDims, now, false),
		metric.NewExt("DockerMemoryLimit", "gauge", 70, baseDims, now, false),
		metric.NewExt("DockerCpuPercentage", "gauge", 0.5, baseDims, now, false),
		metric.NewExt("DockerContainerCount", "counter", 1, expectedDimsGen, now, false),
	}
	d := getSUT()
	d.Configure(config)
	ret := d.buildMetrics(container, stats, 0.5)
	var newMet metric.Metric
	for _, met := range ret {
		newMet = metric.NewExt(met.Name, met.MetricType, met.Value, met.Dimensions, now, false)
		contains(t, expectedMetrics, newMet)
	}
}

func TestDockerStatsBuildMetricsWithBufferRegex(t *testing.T) {
	config := make(map[string]interface{})
	config["bufferRegex"] = "DockerMemory.*"

	stats := new(docker.Stats)
	stats.Networks = make(map[string]docker.NetworkStats)
	stats.Networks["eth0"] = docker.NetworkStats{RxBytes: 10, TxBytes: 20}
	stats.MemoryStats.Usage = 50
	stats.MemoryStats.Limit = 70

	containerJSON := []byte(`
	{
		"ID": "test-id",
		"Name": "test-container",
		"Config": {
			"Env": [
				"MESOS_TASK_ID=my--service.main.blablagit6bdsadnoise"
			]
		}
	}`)
	var container *docker.Container
	err := json.Unmarshal(containerJSON, &container)
	assert.Equal(t, err, nil)

	baseDims := map[string]string{
		"container_id":   "test-id",
		"container_name": "test-container",
	}
	netDims := map[string]string{
		"container_id":   "test-id",
		"container_name": "test-container",
		"iface":          "eth0",
	}

	expectedDimsGen := map[string]string{}
	now := time.Now()
	expectedMetrics := []metric.Metric{
		metric.NewExt("DockerRxBytes", "cumcounter", 10, netDims, now, false),
		metric.NewExt("DockerTxBytes", "cumcounter", 20, netDims, now, false),
		metric.NewExt("DockerMemoryUsed", "gauge", 50, baseDims, now, true),
		metric.NewExt("DockerMemoryLimit", "gauge", 70, baseDims, now, true),
		metric.NewExt("DockerCpuPercentage", "gauge", 0.5, baseDims, now, false),
		metric.NewExt("DockerContainerCount", "counter", 1, expectedDimsGen, now, false),
	}

	d := getSUT()
	d.Configure(config)
	ret := d.buildMetrics(container, stats, 0.5)
	var newMet metric.Metric
	for _, met := range ret {
		newMet = metric.NewExt(met.Name, met.MetricType, met.Value, met.Dimensions, now, met.Buffered)
		contains(t, expectedMetrics, newMet)
	}
}

func TestDockerStatsBuildMetricsWithNameAsEnvVariable(t *testing.T) {
	config := make(map[string]interface{})
	envVars := []byte(`
	{
		"service_name": {
			"SERVICE_NAME": ".*"
		}
	}`)
	var val map[string]interface{}

	err := json.Unmarshal(envVars, &val)
	assert.Equal(t, err, nil)
	config["generatedDimensions"] = val

	stats := new(docker.Stats)
	stats.MemoryStats.Usage = 50
	stats.MemoryStats.Limit = 70

	containerJSON := []byte(`
	{
		"ID": "test-id",
		"Name": "test-container",
		"Config": {
			"Env": [
				"SERVICE_NAME=my_service"
			]
		}
	}`)
	var container *docker.Container
	err = json.Unmarshal(containerJSON, &container)
	assert.Equal(t, err, nil)

	expectedDims := map[string]string{
		"container_id":   "test-id",
		"container_name": "test-container",
		"service_name":   "my_service",
	}
	expectedDimsGen := map[string]string{
		"service_name": "my_service",
	}
	now := time.Now()
	expectedMetrics := []metric.Metric{
		metric.NewExt("DockerRxBytes", "cumcounter", 10, expectedDims, now, false),
		metric.NewExt("DockerTxBytes", "cumcounter", 20, expectedDims, now, false),
		metric.NewExt("DockerMemoryUsed", "gauge", 50, expectedDims, now, false),
		metric.NewExt("DockerMemoryLimit", "gauge", 70, expectedDims, now, false),
		metric.NewExt("DockerCpuPercentage", "gauge", 0.5, expectedDims, now, false),
		metric.NewExt("DockerContainerCount", "counter", 1, expectedDimsGen, now, false),
	}

	d := getSUT()
	d.Configure(config)
	ret := d.buildMetrics(container, stats, 0.5)
	var newMet metric.Metric
	for _, met := range ret {
		newMet = metric.NewExt(met.Name, met.MetricType, met.Value, met.Dimensions, now, false)
		contains(t, expectedMetrics, newMet)
	}
}

func TestDockerStatsCalculateCPUPercent(t *testing.T) {
	var previousTotalUsage = uint64(0)
	var previousSystem = uint64(0)

	stats := new(docker.Stats)
	stats.CPUStats.CPUUsage.PercpuUsage = make([]uint64, 24)
	stats.CPUStats.CPUUsage.TotalUsage = 1261158030354
	stats.CPUStats.SystemCPUUsage = 108086414700000000

	assert.Equal(t, 0.02800332753427522, calculateCPUPercent(previousTotalUsage, previousSystem, stats))

	previousTotalUsage = stats.CPUStats.CPUUsage.TotalUsage
	previousSystem = stats.CPUStats.SystemCPUUsage
	stats.CPUStats.CPUUsage.TotalUsage = 1261164064229
	stats.CPUStats.SystemCPUUsage = 108086652820000000

	assert.Equal(t, 0.060815135225936505, calculateCPUPercent(previousTotalUsage, previousSystem, stats))
}
