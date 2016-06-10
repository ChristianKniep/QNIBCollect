package collector

import (
	"encoding/json"
	"fullerite/config"
	"fullerite/metric"
	"golang.org/x/net/context"
	"io/ioutil"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	l "github.com/Sirupsen/logrus"
	dClient "github.com/docker/engine-api/client"
	dTypes "github.com/docker/engine-api/types"
)

const (
	endpoint = "unix:///var/run/docker.sock"
)

// DockerStats collector type.
// previousCPUValues contains the last cpu-usage values per container.
// dockerClient is the client for the Docker remote API.
type DockerStats struct {
	baseCollector
	previousCPUValues map[string]*CPUValues
	dockerClient      *dClient.Client
	statsTimeout      int
	compiledRegex     map[string]*Regex
	skipRegex         *regexp.Regexp
	bufferRegex       *regexp.Regexp
	endpoint          string
	mu                *sync.Mutex
}

// CPUValues struct contains the last cpu-usage values in order to compute properly the current values.
// (see calculateCPUPercent() for more details)
type CPUValues struct {
	totCPU, systemCPU uint64
}

// Regex struct contains the info used to get the user specific dimensions from the docker env variables
// tag: is the environmental variable you want to get the value from
// regex: is the reg exp used to extract the value from the env var
type Regex struct {
	tag   string
	regex *regexp.Regexp
}

func init() {
	RegisterCollector("DockerStats", newDockerStats)
}

// newDockerStats creates a new DockerStats collector.
func newDockerStats(channel chan metric.Metric, initialInterval int, log *l.Entry) Collector {
	d := new(DockerStats)

	d.log = log
	d.channel = channel
	d.interval = initialInterval
	d.mu = new(sync.Mutex)

	d.name = "DockerStats"
	d.previousCPUValues = make(map[string]*CPUValues)
	d.compiledRegex = make(map[string]*Regex)
	return d
}

// GetEndpoint Returns endpoint of DockerStats instance
func (d *DockerStats) GetEndpoint() string {
	return d.endpoint
}

// Configure takes a dictionary of values with which the handler can configure itself.
func (d *DockerStats) Configure(configMap map[string]interface{}) {
	if timeout, exists := configMap["dockerStatsTimeout"]; exists {
		d.statsTimeout = min(config.GetAsInt(timeout, d.interval), d.interval)
	} else {
		d.statsTimeout = d.interval
	}
	if dockerEndpoint, exists := configMap["dockerEndPoint"]; exists {
		if str, ok := dockerEndpoint.(string); ok {
			d.endpoint = str
		} else {
			d.log.Warn("Failed to cast dokerEndPoint: ", reflect.TypeOf(dockerEndpoint))
		}
	} else {
		d.endpoint = endpoint
	}
	// Set ENV DOCKER_HOST
	d.dockerClient, _ = dClient.NewEnvClient()
	if generatedDimensions, exists := configMap["generatedDimensions"]; exists {
		for dimension, generator := range generatedDimensions.(map[string]interface{}) {
			for key, regx := range config.GetAsMap(generator) {
				re, err := regexp.Compile(regx)
				if err != nil {
					d.log.Warn("Failed to compile regex: ", regx, err)
				} else {
					d.compiledRegex[dimension] = &Regex{regex: re, tag: key}
				}
			}
		}
	}
	if skipRegex, skipExists := configMap["skipContainerRegex"]; skipExists {
		d.skipRegex = regexp.MustCompile(skipRegex.(string))
	}
	if bufferRegex, exists := configMap["bufferRegex"]; exists {
		d.bufferRegex = regexp.MustCompile(bufferRegex.(string))
	}
	d.configureCommonParams(configMap)
}

// Collect iterates on all the docker containers alive and, if possible, collects the correspondent
// memory and cpu statistics.
// For each container a gorutine is started to spin up the collection process.
func (d *DockerStats) Collect() {
	options := dTypes.ContainerListOptions{All: false}
	containers, err := d.dockerClient.ContainerList(context.Background(), options)
	if err != nil {
		d.log.Error("ListContainers() failed: ", err)
		return
	}
	for _, cnt := range containers {
		contName := strings.TrimPrefix(cnt.Names[0], "/")
		if err != nil {
			d.log.Error("InspectContainer() failed: ", err)
			continue
		}

		if d.skipRegex != nil && d.skipRegex.MatchString(contName) {
			d.log.Debug("Skip container: ", contName)
			continue
		}
		if _, ok := d.previousCPUValues[cnt.ID]; !ok {
			d.previousCPUValues[cnt.ID] = new(CPUValues)
		}
		go d.getDockerContainerInfo(cnt)
	}
}

// getDockerContainerInfo gets container statistics for the given container.
// results is a channel to make possible the synchronization between the main process and the gorutines (wait-notify pattern).
func (d *DockerStats) getDockerContainerInfo(container dTypes.Container) {
	errC := make(chan error, 1)
	statsC := make(chan []byte, 1)
	done := make(chan bool, 1)

	go func() {
		body, _ := d.dockerClient.ContainerStats(context.Background(), container.ID, false)
		defer body.Close()
		stats, _ := ioutil.ReadAll(body)
		statsC <- stats
	}()
	select {
	case stats, ok := <-statsC:
		if !ok {
			err := <-errC
			d.log.Error("Failed to collect docker container stats: ", err)
			break
		}
		done <- true
		metrics := d.extractMetrics(container, stats)
		d.sendMetrics(metrics)
		break
	case <-time.After(time.Duration(d.statsTimeout) * time.Second):
		d.log.Error("Timed out collecting stats for container ", container.ID)
		done <- true
		break
	}
}

func (d *DockerStats) extractMetrics(container dTypes.Container, stats []byte) []metric.Metric {
	d.mu.Lock()
	defer d.mu.Unlock()
	var dstats dTypes.Stats
	err := json.Unmarshal(stats, &dstats)
	if err != nil {
		d.log.Warn("Something went wrong...", err)
	}
	metrics := d.buildMetrics(container, dstats, calculateCPUPercent(d.previousCPUValues[container.ID].totCPU, d.previousCPUValues[container.ID].systemCPU, dstats))

	d.previousCPUValues[container.ID].totCPU = dstats.CPUStats.CPUUsage.TotalUsage
	d.previousCPUValues[container.ID].systemCPU = dstats.CPUStats.SystemUsage
	return metrics
}

// buildMetrics creates the actual metrics for the given container.
func (d DockerStats) buildMetrics(container dTypes.Container, containerStats dTypes.Stats, cpuPercentage float64) []metric.Metric {
	ret := []metric.Metric{
		d.buildDockerMetric("DockerMemoryUsed", metric.Gauge, float64(containerStats.MemoryStats.Usage)),
		d.buildDockerMetric("DockerMemoryLimit", metric.Gauge, float64(containerStats.MemoryStats.Limit)),
		d.buildDockerMetric("DockerCpuPercentage", metric.Gauge, cpuPercentage),
	}
	/*for netiface := range containerStats.Networks {
		// legacy format
		txb := d.buildDockerMetric("DockerTxBytes", metric.CumulativeCounter, float64(containerStats.Networks[netiface].TxBytes))
		txb.AddDimension("iface", netiface)
		ret = append(ret, txb)
		rxb := d.buildDockerMetric("DockerRxBytes", metric.CumulativeCounter, float64(containerStats.Networks[netiface].RxBytes))
		rxb.AddDimension("iface", netiface)
		ret = append(ret, rxb)
	}*/

	additionalDimensions := map[string]string{
		"container_id":   container.ID,
		"container_name": strings.TrimPrefix(container.Names[0], "/"),
	}
	metric.AddToAll(&ret, additionalDimensions)
	/*
		ret = append(ret, d.buildDockerMetric("DockerContainerCount", metric.Counter, 1))
		// TODO: Do we need this?
		// metric.AddToAll(&ret, d.extractDimensions(container))
	*/
	return ret
}

// sendMetrics writes all the metrics received to the collector channel.
func (d DockerStats) sendMetrics(metrics []metric.Metric) {
	for _, m := range metrics {
		d.Channel() <- m
	}
}

// Function that extracts additional dimensions from the docker environmental variables set up by the user
// in the configuration file.
/*func (d DockerStats) extractDimensions(container dTypes.Container) map[string]string {
	envVars := container.Config.Env
	ret := map[string]string{}

	for dimension, r := range d.compiledRegex {
		for _, envVariable := range envVars {
			envArray := strings.Split(envVariable, "=")
			if r.tag == envArray[0] {
				subMatch := r.regex.FindStringSubmatch(envArray[1])
				if len(subMatch) > 0 {
					ret[dimension] = strings.Replace(subMatch[len(subMatch)-1], "--", "_", -1)
				}
			}
		}
	}
	d.log.Debug(ret)
	return ret
}
*/

func (d DockerStats) buildDockerMetric(name string, metricType string, value float64) (m metric.Metric) {
	m = metric.New(name)
	m.MetricType = metricType
	m.Value = value
	m.SetTime(time.Now())
	if d.bufferRegex != nil && d.bufferRegex.MatchString(name) {
		m.Buffered = true
	}
	return m
}

// Function that compute the current cpu usage percentage combining current and last values.
func calculateCPUPercent(previousCPU, previousSystem uint64, stats dTypes.Stats) float64 {
	var (
		cpuPercent = 0.0
		// calculate the change for the cpu usage of the container in between readings
		cpuDelta = float64(stats.CPUStats.CPUUsage.TotalUsage - previousCPU)
		// calculate the change for the entire system between readings
		systemDelta = float64(stats.CPUStats.SystemUsage - previousSystem)
	)

	if systemDelta > 0.0 && cpuDelta > 0.0 {
		cpuPercent = (cpuDelta / systemDelta) * float64(len(stats.CPUStats.CPUUsage.PercpuUsage)) * 100.0
	}
	return cpuPercent
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
