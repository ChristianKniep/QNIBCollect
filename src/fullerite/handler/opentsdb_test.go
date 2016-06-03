package handler

import (
	"fullerite/metric"

	"fmt"
	"testing"
	"time"

	l "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func getTestOpenTSDBHandler(interval, buffsize, timeoutsec int) *OpenTSDBHandler {
	testChannel := make(chan metric.Metric)
	testLog := l.WithField("testing", "graphite_handler")
	timeout := time.Duration(timeoutsec) * time.Second

	return newOpenTSDBHandler(testChannel, interval, buffsize, timeout, testLog).(*OpenTSDBHandler)
}

func TestOpenTSDBHandlerConfigureEmptyConfig(t *testing.T) {
	config := make(map[string]interface{})
	g := getTestOpenTSDBHandler(12, 13, 14)
	g.Configure(config)

	assert.Equal(t, 12, g.Interval())
}

func TestOpenTSDBHandlerConfigure(t *testing.T) {
	config := map[string]interface{}{
		"interval":        "10",
		"timeout":         "10",
		"max_buffer_size": "100",
		"server":          "test_server",
		"port":            "10101",
	}

	g := getTestOpenTSDBHandler(12, 13, 14)
	g.Configure(config)

	assert.Equal(t, 10, g.Interval())
	assert.Equal(t, 100, g.MaxBufferSize())
	assert.Equal(t, "test_server", g.Server())
	assert.Equal(t, "10101", g.Port())
}

func TestOpenTSDBHandlerConfigureIntPort(t *testing.T) {
	config := map[string]interface{}{
		"interval":        "10",
		"timeout":         "10",
		"max_buffer_size": "100",
		"server":          "test_server",
		"port":            10101,
	}

	g := getTestOpenTSDBHandler(12, 13, 14)
	g.Configure(config)

	assert.Equal(t, 10, g.Interval())
	assert.Equal(t, 100, g.MaxBufferSize())
	assert.Equal(t, "test_server", g.Server())
	assert.Equal(t, "10101", g.Port())
}

// TestConvertToOpenTSDBHandler tests the plain handler convertion
func TestConvertToOpenTSDBHandler(t *testing.T) {
	config := map[string]interface{}{
		"interval":        "10",
		"timeout":         "10",
		"max_buffer_size": "100",
		"server":          "test_server",
		"port":            10101,
	}

	g := getTestOpenTSDBHandler(12, 13, 14)
	g.Configure(config)

	waitForSplitSecond()
	now := time.Now().Unix()
	m := metric.New("TestMetric")

	dpString := g.convertToOpenTSDBHandler(m)

	assert.Equal(t, fmt.Sprintf("put TestMetric %d 0.000000\n", now), dpString)
}

// TestConvertToOpenTSDBHandlerDims tests the handler convertion with dimensions
func TestConvertToOpenTSDBHandlerDims(t *testing.T) {
	config := map[string]interface{}{
		"server": "test_server",
		"port":   10101,
	}

	g := getTestOpenTSDBHandler(12, 13, 14)
	g.Configure(config)

	waitForSplitSecond()
	now := time.Now().Unix()
	m := metric.New("TestMetric")

	dims := map[string]string{
		"container_id":   "test-id",
		"container_name": "test-container",
	}
	m.Dimensions = dims

	dpString := g.convertToOpenTSDBHandler(m)

	assert.Equal(t, fmt.Sprintf("put TestMetric %d 0.000000 container_id=test-id,container_name=test-container\n", now), dpString)
}

// TestConvertToOpenTSDBHandlerDimsOldFormat tests the handler convertion with dimensions
func TestConvertToOpenTSDBHandlerDimsOldFormat(t *testing.T) {
	config := map[string]interface{}{
		"server":    "test_server",
		"port":      10101,
		"oldformat": true,
	}

	g := getTestOpenTSDBHandler(12, 13, 14)
	g.Configure(config)

	waitForSplitSecond()
	now := time.Now().Unix()
	m := metric.New("TestMetric")

	dims := map[string]string{
		"container_id":   "test-id",
		"container_name": "test-container",
	}
	m.Dimensions = dims

	dpString := g.convertToOpenTSDBHandler(m)

	assert.Equal(t, fmt.Sprintf("put TestMetric container_id=test-id,container_name=test-container %d 0.000000\n", now), dpString)
}
