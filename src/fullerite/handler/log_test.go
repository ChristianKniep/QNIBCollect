package handler

import (
	"fmt"
	"fullerite/metric"
	"time"

	l "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
)

func getTestLogHandler(interval int, buffsize int) *Log {
	testChannel := make(chan metric.Metric)
	testLog := l.WithField("testing", "log_handler")

	return newLog(testChannel, interval, buffsize, time.Duration(1)*time.Second, testLog).(*Log)
}

func TestLogConfigureEmptyConfig(t *testing.T) {
	config := make(map[string]interface{})
	h := getTestLogHandler(12, 13)
	h.Configure(config)

	assert.Equal(t, 12, h.Interval())
	assert.Equal(t, 13, h.MaxBufferSize())
}

func TestLogConfigure(t *testing.T) {
	config := map[string]interface{}{
		"interval":        "10",
		"max_buffer_size": "100",
	}

	h := getTestLogHandler(12, 13)
	h.Configure(config)

	assert.Equal(t, 10, h.Interval())
	assert.Equal(t, 100, h.MaxBufferSize())
}

func TestConvertToLog(t *testing.T) {
	now := time.Now()
	h := getTestLogHandler(12, 13)
	m := metric.New("TestMetric")
	m.SetTime(now)

	dpString, err := h.convertToLog(m)
	if err != nil {
		t.Errorf("convertToLog failed to convert %q: err", m, err)
	}
	nowFmt := now.Format(time.RFC3339Nano)
	assert.Equal(t, fmt.Sprintf("{\"name\":\"TestMetric\",\"type\":\"gauge\",\"value\":0,\"dimensions\":{},\"buffered\":false,\"time\":\"%s\",\"agg-enabled\":false}", nowFmt), dpString)
}
