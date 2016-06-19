package handler

import (
	"fmt"
	"fullerite/metric"
	"log"
	"time"

	l "github.com/Sirupsen/logrus"
	"github.com/influxdata/influxdb/client/v2"
)

func init() {
	RegisterHandler("InfluxDB", newInfluxDB)
}

// InfluxDB type
type InfluxDB struct {
	BaseHandler
	server   string
	port     string
	database string
	username string
	password string
	influxdb client.Client
}

// newInfluxDB returns a new InfluxDB handler.
func newInfluxDB(
	channel chan metric.Metric,
	initialInterval int,
	initialBufferSize int,
	initialTimeout time.Duration,
	log *l.Entry) Handler {
	inst := new(InfluxDB)
	inst.name = "InfluxDB"
	inst.interval = initialInterval
	inst.maxBufferSize = initialBufferSize
	inst.timeout = initialTimeout
	inst.log = log
	inst.channel = channel

	return inst
}

// Server returns the InfluxDB server's name or IP
func (i InfluxDB) Server() string {
	return i.server
}

// Port returns the InfluxDB server's port number
func (i InfluxDB) Port() string {
	return i.port
}

// Configure accepts the different configuration options for the InfluxDB handler
func (i *InfluxDB) Configure(configMap map[string]interface{}) {
	if server, exists := configMap["server"]; exists {
		i.server = server.(string)
	} else {
		i.log.Error("There was no server specified for the InfluxDB Handler, there won't be any emissions")
	}

	if port, exists := configMap["port"]; exists {
		i.port = fmt.Sprint(port)
	} else {
		i.log.Error("There was no port specified for the InfluxDB Handler, there won't be any emissions")
	}
	if username, exists := configMap["username"]; exists {
		i.username = username.(string)
	} else {
		i.log.Error("There was no user specified for the InfluxDB Handler, there won't be any emissions")
	}
	if password, exists := configMap["password"]; exists {
		i.password = password.(string)
	} else {
		i.log.Error("There was no password specified for the InfluxDB Handler, there won't be any emissions")
	}
	if database, exists := configMap["database"]; exists {
		i.database = database.(string)
	} else {
		i.log.Error("There was no database specified for the InfluxDB Handler, there won't be any emissions")
	}
	// Make client
	addr := fmt.Sprintf("http://%s:%s", i.server, i.port)

	var err error
	i.influxdb, err = client.NewHTTPClient(client.HTTPConfig{
		Addr:     addr,
		Username: i.username,
		Password: i.password,
	})
	if err != nil {
		i.log.Warn("Error: ", err)
	}
	i.configureCommonParams(configMap)
}

// Run runs the handler main loop
func (i *InfluxDB) Run() {
	i.run(i.emitMetrics)
}

func (i InfluxDB) convertToInfluxDB(incomingMetric metric.Metric) (datapoint *client.Point) {
	tags := incomingMetric.GetDimensions(i.DefaultDimensions())
	// Assemble field (could be improved to convey multiple fields)
	fields := map[string]interface{}{
		"value": incomingMetric.Value,
	}
	pt, err := client.NewPoint(incomingMetric.Name, tags, fields, incomingMetric.Time)
	if err != nil {
		log.Fatalln("Error: ", err)
	}
	return pt
}

func (i *InfluxDB) emitMetrics(metrics []metric.Metric) bool {
	i.log.Info("Starting to emit ", len(metrics), " metrics")

	if len(metrics) == 0 {
		i.log.Warn("Skipping send because of an empty payload")
		return false
	}

	// Create a new point batch to be send in bulk
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  i.database,
		Precision: "s",
	})
	if err != nil {
		log.Fatalln("Error: ", err)
	}

	//iterate over metrics
	for _, m := range metrics {
		bp.AddPoint(i.convertToInfluxDB(m))
	}

	// Write the batch
	i.influxdb.Write(bp)
	return true
}
