package handler

import (
	"fmt"
	"fullerite/metric"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	l "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func getTestInfluxDBHandler(interval, buffsize, timeoutsec int) *InfluxDB {
	testChannel := make(chan metric.Metric)
	testLog := l.WithField("testing", "influxdb_handler")
	timeout := time.Duration(timeoutsec) * time.Second

	return newInfluxDB(testChannel, interval, buffsize, timeout, testLog).(*InfluxDB)
}

func TestInfluxDBConfigureEmptyConfig(t *testing.T) {
	config := make(map[string]interface{})
	h := getTestInfluxDBHandler(12, 13, 14)
	h.Configure(config)

	assert.Equal(t, 12, h.Interval())
}

func TestInfluxDBConfigure(t *testing.T) {
	config := map[string]interface{}{
		"interval":        "10",
		"timeout":         "10",
		"max_buffer_size": "100",
		"server":          "test_server",
		"port":            "8086",
		"database":        "influxdb",
		"username":        "root",
		"password":        "root",
	}

	i := getTestInfluxDBHandler(12, 13, 14)
	i.Configure(config)

	assert.Equal(t, 10, i.Interval())
	assert.Equal(t, 100, i.MaxBufferSize())
	assert.Equal(t, "test_server", i.Server())
	assert.Equal(t, "8086", i.Port())
}

func TestInfluxDBConfigureIntPort(t *testing.T) {
	config := map[string]interface{}{
		"interval":        "10",
		"timeout":         "10",
		"max_buffer_size": "100",
		"server":          "test_server",
		"port":            "8086",
	}

	i := getTestInfluxDBHandler(12, 13, 14)
	i.Configure(config)

	assert.Equal(t, 10, i.Interval())
	assert.Equal(t, 100, i.MaxBufferSize())
	assert.Equal(t, "test_server", i.Server())
	assert.Equal(t, "8086", i.Port())
}

// TestConvertToInfluxDB tests the plain handler convertion
func TestConvertToInfluxDB(t *testing.T) {
	config := map[string]interface{}{
		"interval":        "10",
		"timeout":         "10",
		"max_buffer_size": "100",
		"server":          "test_server",
		"port":            "8086",
	}

	i := getTestInfluxDBHandler(12, 13, 14)
	i.Configure(config)

	start := time.Now().UnixNano()
	// Create Metric
	m := metric.New("TestMetric")
	// Create datapoint
	pt := i.convertToInfluxDB(m)
	dbString := "TestMetric value=0 [0-9]+"
	fmt.Println(dbString)
	msg := fmt.Sprintf("'%s' does not match expected '%s'", pt.String(), dbString)
	match, _ := regexp.MatchString(dbString, pt.String())
	assert.True(t, match, msg)
	// Testing the timestamp
	l := strings.Split(pt.String(), " ")
	ts := l[len(l)-1]
	tsInt, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		// handle error
		fmt.Println(err)
		assert.True(t, false, fmt.Sprintf("could not convert '%s' to int", ts))
	}
	end := time.Now().UnixNano()
	msg = fmt.Sprintf("Timestamp is not within barriers. start:%d, ts:%d, now:%d", start, tsInt, end)
	assert.True(t, (start < tsInt) && (tsInt < end), msg)

}
