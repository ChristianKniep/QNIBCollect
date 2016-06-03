package handler

import (
	"fmt"
	"fullerite/metric"
	"net"
	"sort"
	"strings"
	"time"

	l "github.com/Sirupsen/logrus"
)

func init() {
	RegisterHandler("OpenTSDBHandler", newOpenTSDBHandler)
}

// OpenTSDBHandler type
type OpenTSDBHandler struct {
	BaseHandler
	server    string
	port      string
	oldFormat bool
}

// newOpenTSDBHandler returns a new Graphite handler.
func newOpenTSDBHandler(
	channel chan metric.Metric,
	initialInterval int,
	initialBufferSize int,
	initialTimeout time.Duration,
	log *l.Entry) Handler {

	inst := new(OpenTSDBHandler)
	inst.name = "OpenTSDBHandler"

	inst.interval = initialInterval
	inst.maxBufferSize = initialBufferSize
	inst.timeout = initialTimeout
	inst.log = log
	inst.channel = channel

	return inst
}

// Server returns the OpenTSDBHandler server's name or IP
func (h OpenTSDBHandler) Server() string {
	return h.server
}

// Port returns the Graphite server's port number
func (h OpenTSDBHandler) Port() string {
	return h.port
}

// Configure accepts the different configuration options for the OpenTSDBHandler handler
func (h *OpenTSDBHandler) Configure(configMap map[string]interface{}) {
	if server, exists := configMap["server"]; exists {
		h.server = server.(string)
	} else {
		h.log.Error("There was no server specified for the OpenTSDB Handler, there won't be any emissions")
	}

	if port, exists := configMap["port"]; exists {
		h.port = fmt.Sprint(port)
	} else {
		h.log.Error("There was no port specified for the OpenTSDB Handler, there won't be any emissions")
	}
	if oldf, exists := configMap["oldformat"]; exists {
		h.oldFormat = oldf.(bool)
	} else {
		h.oldFormat = false
	}
	h.configureCommonParams(configMap)
}

// Run runs the handler main loop
func (h *OpenTSDBHandler) Run() {
	h.run(h.emitMetrics)
}

func (h OpenTSDBHandler) convertToOpenTSDBHandler(incomingMetric metric.Metric) (datapoint string) {
	//orders dimensions so datapoint keeps consistent name
	var keys []string
	dimensions := incomingMetric.GetDimensions(h.DefaultDimensions())
	for k := range dimensions {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	datapoint = fmt.Sprintf("put %s", incomingMetric.Name)
	var dims []string
	for _, key := range keys {
		dims = append(dims, fmt.Sprintf("%s=%s", key, dimensions[key]))
	}
	if len(dimensions) == 0 {
		datapoint = fmt.Sprintf("%s %d %f\n", datapoint, incomingMetric.GetTime().Unix(), incomingMetric.Value)
	} else {
		if h.oldFormat {
			datapoint = fmt.Sprintf("%s %s %d %f\n", datapoint, strings.Join(dims[:], ","), incomingMetric.GetTime().Unix(), incomingMetric.Value)
		} else {
			datapoint = fmt.Sprintf("%s %d %f %s\n", datapoint, incomingMetric.GetTime().Unix(), incomingMetric.Value, strings.Join(dims[:], ","))
		}
	}
	return datapoint
}

func (h *OpenTSDBHandler) emitMetrics(metrics []metric.Metric) bool {
	h.log.Info("Starting to emit ", len(metrics), " metrics")

	if len(metrics) == 0 {
		h.log.Warn("Skipping send because of an empty payload")
		return false
	}

	addr := fmt.Sprintf("%s:%s", h.server, h.port)
	conn, err := net.DialTimeout("tcp", addr, h.timeout)
	if err != nil {
		h.log.Error("Failed to connect ", addr)
		return false
	}

	for _, m := range metrics {
		str := h.convertToOpenTSDBHandler(m)
		h.log.Debug(str)
		fmt.Fprintf(conn, str)
	}
	return true
}
