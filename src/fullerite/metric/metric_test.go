package metric_test

import (
	"encoding/json"
	"fullerite/metric"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewMetric(t *testing.T) {
	m := metric.New("TestMetric")

	assert := assert.New(t)
	assert.Equal(m.Name, "TestMetric")
	assert.Equal(m.Value, 0.0, "default value should be 0.0")
	assert.Equal(m.MetricType, "gauge", "should be a Gauge metric")
	assert.NotEqual(len(m.Dimensions), 1, "should have one dimension")
	assert.False(m.Buffered, "should be unbuffered")
}

func TestMetricBuffering(t *testing.T) {
	m := metric.New("TestMetric")
	m.EnableBuffering()
	assert.True(t, m.Buffered, "should be buffered")
	m.DisableBuffering()
	assert.False(t, m.Buffered, "should be unbuffered")
}
func TestAddDimension(t *testing.T) {
	m := metric.New("TestMetric")
	m.AddDimension("TestDimension", "test value")

	assert := assert.New(t)
	assert.Equal(len(m.Dimensions), 1, "should have 1 dimension")
	assert.Equal(m.Dimensions["TestDimension"], "test value")
}

func TestRemoveDimension(t *testing.T) {
	m := metric.New("TestMetric")
	m.AddDimension("TestDimension", "test value")
	m.AddDimension("TestDimension1", "test value")

	assert := assert.New(t)
	assert.Equal(len(m.Dimensions), 2, "should have 2 dimensions")
	m.RemoveDimension("TestDimension1")
	assert.Equal(len(m.Dimensions), 1, "should have 1 dimension")
	assert.Equal(m.Dimensions["TestDimension"], "test value")
}

func TestGetDimensionsWithNoDimensions(t *testing.T) {
	defaultDimensions := make(map[string]string)
	m := metric.New("TestMetric")

	assert.Equal(t, len(m.GetDimensions(defaultDimensions)), 0)
}

func TestGetDimensionsWithDimensions(t *testing.T) {
	defaultDimensions := make(map[string]string)
	defaultDimensions["DefaultDim"] = "default value"
	m := metric.New("TestMetric")
	m.AddDimension("TestDimension", "test value")

	numDimensions := len(m.GetDimensions(defaultDimensions))
	assert.Equal(t, numDimensions, 2, "dimensions length should be 2")
}

func TestGetDimensionValueFound(t *testing.T) {
	m := metric.New("TestMetric")
	m.AddDimension("TestDimension", "test value")
	value, ok := m.GetDimensionValue("TestDimension")

	assert := assert.New(t)
	assert.Equal(value, "test value", "test value does not match")
	assert.Equal(ok, true, "should succeed")
}

func TestGetDimensionValueNotFound(t *testing.T) {
	m := metric.New("TestMetric")
	value, ok := m.GetDimensionValue("TestDimension")

	assert := assert.New(t)
	assert.Equal(value, "", "non-existing value should be empty")
	assert.Equal(ok, false, "should return false")
}

func TestSanitizeMetricNameColon(t *testing.T) {
	m := metric.New("DirtyMetric:")
	assert.Equal(t, "DirtyMetric-", m.Name, "metric name should be sanitized")
}

func TestSanitizeMetricNameEqual(t *testing.T) {
	m := metric.New("DirtyMetric=")
	assert.Equal(t, "DirtyMetric-", m.Name, "metric name should be sanitized")
}

func TestSanitizeDimensionNameColon(t *testing.T) {
	defaultDimensions := make(map[string]string)
	m := metric.New("TestMetric")
	m.AddDimension("DirtyDimension:", "dimension value")
	assert := assert.New(t)

	value, ok := m.Dimensions["DirtyDimension-"]
	assert.Equal("dimension value", value, "dimension value does not match")
	assert.True(ok)

	value, ok = m.Dimensions["DirtyDimension:"]
	assert.False(ok)

	value, ok = m.GetDimensionValue("DirtyDimension:")
	assert.Equal("dimension value", value, "dimension value does not match")
	assert.True(ok)

	value, ok = m.GetDimensionValue("DirtyDimension-")
	assert.Equal("dimension value", value, "dimension value does not match")
	assert.True(ok)

	dimensions := m.GetDimensions(defaultDimensions)
	value, ok = dimensions["DirtyDimension:"]
	assert.False(ok)

	value, ok = dimensions["DirtyDimension-"]
	assert.Equal("dimension value", value, "dimension value does not match")
	assert.True(ok)
}

func TestSanitizeDimensionNameEqual(t *testing.T) {
	defaultDimensions := make(map[string]string)
	m := metric.New("TestMetric")
	m.AddDimension("DirtyDimension=", "dimension value")
	assert := assert.New(t)

	value, ok := m.Dimensions["DirtyDimension-"]
	assert.Equal("dimension value", value, "dimension value does not match")
	assert.True(ok)

	value, ok = m.Dimensions["DirtyDimension="]
	assert.False(ok)

	value, ok = m.GetDimensionValue("DirtyDimension=")
	assert.Equal("dimension value", value, "dimension value does not match")
	assert.True(ok)

	value, ok = m.GetDimensionValue("DirtyDimension-")
	assert.Equal("dimension value", value, "dimension value does not match")
	assert.True(ok)

	dimensions := m.GetDimensions(defaultDimensions)
	value, ok = dimensions["DirtyDimension="]
	assert.False(ok)

	value, ok = dimensions["DirtyDimension-"]
	assert.Equal("dimension value", value, "dimension value does not match")
	assert.True(ok)
}

func TestSanitizeDimensionValueColon(t *testing.T) {
	defaultDimensions := make(map[string]string)
	m := metric.New("TestMetric")
	m.AddDimension("TestDimension", "dirty value:")
	assert := assert.New(t)

	value, ok := m.Dimensions["TestDimension"]
	assert.Equal("dirty value-", value, "dimension value does not match")
	assert.True(ok)

	value, ok = m.GetDimensionValue("TestDimension")
	assert.Equal("dirty value-", value, "dimension value does not match")
	assert.True(ok)

	dimensions := m.GetDimensions(defaultDimensions)
	value, ok = dimensions["TestDimension"]
	assert.Equal("dirty value-", value, "dimension value does not match")
	assert.True(ok)
}

func TestSanitizeDimensionValueEqual(t *testing.T) {
	defaultDimensions := make(map[string]string)
	m := metric.New("TestMetric")
	m.AddDimension("TestDimension", "dirty value=")
	assert := assert.New(t)

	value, ok := m.Dimensions["TestDimension"]
	assert.Equal("dirty value-", value, "dimension value does not match")
	assert.True(ok)

	value, ok = m.GetDimensionValue("TestDimension")
	assert.Equal("dirty value-", value, "dimension value does not match")
	assert.True(ok)

	dimensions := m.GetDimensions(defaultDimensions)
	value, ok = dimensions["TestDimension"]
	assert.Equal("dirty value-", value, "dimension value does not match")
	assert.True(ok)
}

func TestSanitizeMultiple(t *testing.T) {
	defaultDimensions := make(map[string]string)
	m := metric.New(":=Dirty==::Metric=:")
	m.AddDimension(":=Dirty==::Dimension=:", ":=dirty==::value=:")
	m.AddDimension(":=Dirty==::Dimension=:2", ":=another==dirty::value=:")
	assert := assert.New(t)

	assert.Equal("--Dirty----Metric--", m.Name, "metric name should be sanitized")

	value, ok := m.GetDimensionValue(":=Dirty==::Dimension=:")
	assert.Equal("--dirty----value--", value, "dimension value does not match")
	assert.True(ok)

	value, ok = m.GetDimensionValue("--Dirty----Dimension--")
	assert.Equal("--dirty----value--", value, "dimension value does not match")
	assert.True(ok)

	value, ok = m.GetDimensionValue(":=Dirty==::Dimension=:2")
	assert.Equal("--another--dirty--value--", value, "dimension value does not match")
	assert.True(ok)

	value, ok = m.GetDimensionValue("--Dirty----Dimension--2")
	assert.Equal("--another--dirty--value--", value, "dimension value does not match")
	assert.True(ok)

	dimensions := m.GetDimensions(defaultDimensions)

	value, ok = dimensions[":=Dirty==::Dimension=:"]
	assert.False(ok)

	value, ok = dimensions[":=Dirty==::Dimension=:2"]
	assert.False(ok)

	value, ok = dimensions["--Dirty----Dimension--"]
	assert.Equal("--dirty----value--", value, "dimension value does not match")
	assert.True(ok)

	value, ok = dimensions["--Dirty----Dimension--2"]
	assert.Equal("--another--dirty--value--", value, "dimension value does not match")
	assert.True(ok)
}

func TestSanitizeDimensionNameOverwriteDirtyDirty(t *testing.T) {
	defaultDimensions := make(map[string]string)
	m := metric.New("TestMetric")
	m.AddDimension("Test=Dimension", "first value")
	m.AddDimension("Test:Dimension", "second value")
	assert := assert.New(t)

	value, ok := m.Dimensions["Test-Dimension"]
	assert.Equal("second value", value, "dimension value does not match")
	assert.True(ok)

	value, ok = m.Dimensions["Test=Dimension"]
	assert.False(ok)
	value, ok = m.Dimensions["Test:Timension"]
	assert.False(ok)

	value, ok = m.GetDimensionValue("Test=Dimension")
	assert.Equal("second value", value, "dimension value does not match")
	assert.True(ok)
	value, ok = m.GetDimensionValue("Test:Dimension")
	assert.Equal("second value", value, "dimension value does not match")
	assert.True(ok)
	value, ok = m.GetDimensionValue("Test-Dimension")
	assert.Equal("second value", value, "dimension value does not match")
	assert.True(ok)

	dimensions := m.GetDimensions(defaultDimensions)
	value, ok = dimensions["Test-Dimension"]
	assert.Equal("second value", value, "dimension value does not match")
	assert.True(ok)
	value, ok = dimensions["Test=Dimension"]
	assert.False(ok)
	value, ok = dimensions["Test:Dimension"]
	assert.False(ok)

	assert.Equal(1, len(dimensions), "only 1 dimension should exist")
}

func TestSanitizeDimensionNameOverwriteDirtyClean(t *testing.T) {
	defaultDimensions := make(map[string]string)
	m := metric.New("TestMetric")
	m.AddDimension("Test=Dimension", "first value")
	m.AddDimension("Test-Dimension", "second value")
	assert := assert.New(t)

	value, ok := m.Dimensions["Test-Dimension"]
	assert.Equal("second value", value, "dimension value does not match")
	assert.True(ok)

	value, ok = m.Dimensions["Test=Dimension"]
	assert.False(ok)

	value, ok = m.GetDimensionValue("Test=Dimension")
	assert.Equal("second value", value, "dimension value does not match")
	assert.True(ok)
	value, ok = m.GetDimensionValue("Test-Dimension")
	assert.Equal("second value", value, "dimension value does not match")
	assert.True(ok)

	dimensions := m.GetDimensions(defaultDimensions)
	value, ok = dimensions["Test-Dimension"]
	assert.Equal("second value", value, "dimension value does not match")
	assert.True(ok)
	value, ok = dimensions["Test=Dimension"]
	assert.False(ok)

	assert.Equal(1, len(dimensions), "only 1 dimension should exist")
}

func TestSanitizeDimensionNameOverwriteCleanDirty(t *testing.T) {
	defaultDimensions := make(map[string]string)
	m := metric.New("TestMetric")
	m.AddDimension("Test-Dimension", "first value")
	m.AddDimension("Test=Dimension", "second value")
	assert := assert.New(t)

	value, ok := m.Dimensions["Test-Dimension"]
	assert.Equal("second value", value, "dimension value does not match")
	assert.True(ok)

	value, ok = m.Dimensions["Test=Dimension"]
	assert.False(ok)

	value, ok = m.GetDimensionValue("Test=Dimension")
	assert.Equal("second value", value, "dimension value does not match")
	assert.True(ok)
	value, ok = m.GetDimensionValue("Test-Dimension")
	assert.Equal("second value", value, "dimension value does not match")
	assert.True(ok)

	dimensions := m.GetDimensions(defaultDimensions)
	value, ok = dimensions["Test-Dimension"]
	assert.Equal("second value", value, "dimension value does not match")
	assert.True(ok)
	value, ok = dimensions["Test=Dimension"]
	assert.False(ok)

	assert.Equal(1, len(dimensions), "only 1 dimension should exist")
}

func TestAddDimensions(t *testing.T) {
	m1 := metric.New("TestMetric")
	m2 := metric.New("TestMetric")
	m2.SetTime(m1.GetTime())
	dimensions := map[string]string{
		"TestDimension":    "TestValue",
		"Dirty:=Dimension": "Dirty:=Value",
	}
	m1.AddDimension("TestDimension", "TestValue")
	m1.AddDimension("Dirty:=Dimension", "Dirty:=Value")
	m2.AddDimensions(dimensions)

	assert.Equal(t, m1, m2)
}

func TestToJSON(t *testing.T) {
	m := metric.New("TestMetric")
	s := m.ToJSON()
	var nm metric.Metric
	json.Unmarshal([]byte(s), &nm)
	assert := assert.New(t)
	assert.Equal(m, nm)
}

func TestIsSubDim(t *testing.T) {
	m := metric.New("TestMetric")
	m.AddDimension("dim1", "1")
	m.AddDimension("dim2", "2")
	m.AddDimension("dim3", "3")
	other := map[string]string{
		"dim1": "1",
		"dim2": "2",
		"dim3": "3",
	}
	assert.True(t, m.IsSubDim(other), "identical dimension")
	other = map[string]string{
		"dim1": "1",
		"dim2": "2",
	}
	assert.True(t, m.IsSubDim(other), "other is smaller then my")
	other = map[string]string{
		"dim1": "1",
		"dim2": "2",
		"dim3": "3",
		"dim4": "4",
	}
	assert.False(t, m.IsSubDim(other), "other is bigger then my")
	other = map[string]string{
		"dim1": "1",
		"dim2": "2",
		"dim3": "4",
	}
	assert.False(t, m.IsSubDim(other), "other[dim3] has different key then my[dim3]")
}

func TestGetFilterID(t *testing.T) {
	m := metric.New("TestMetric")
	m.AddDimension("dim1", "1")
	m.AddDimension("dim2", "2")
	assert.Equal(t, "TestMetric_gauge_dim1:1_dim2:2", m.GetFilterID())
}

func TestIsFiltered(t *testing.T) {
	m := metric.New("TestMetric")
	m.AddDimension("dim1", "1")
	m.AddDimension("dim2", "2")
	good := map[string]string{
		"dim1": "1",
	}
	f := metric.NewFilter("Test.*", "gauge", good)
	assert.True(t, m.IsFiltered(f), "Should map")
	bad := map[string]string{
		"dim1": "2",
	}
	f = metric.NewFilter("Test.*", "counter", good)
	assert.False(t, m.IsFiltered(f), "Should not map due to Type")
	f = metric.NewFilter("Test.*", "gauge", bad)
	assert.False(t, m.IsFiltered(f), "Should not map due to dimensions")
	f = metric.NewFilter("Fail.*", "gauge", good)
	assert.False(t, m.IsFiltered(f), "Should not map due to Name")
}

func TestAggregateList(t *testing.T) {
	ms := []metric.Metric{}
	d := map[string]string{
		"dim1": "1",
	}
	var lastT time.Time
	for i := 0; i < 5; i++ {
		lastT = time.Now()
		m := metric.NewExt("test", "gauge", float64(i), d, lastT, false)
		ms = append(ms, m)
		time.Sleep(100 * time.Millisecond)
	}
	res := metric.AggregateList(ms)
	exp := []metric.Metric{
		metric.NewExt("test", "gauge", 2.0, d, lastT, false),
	}
	assert.Equal(t, exp, res)
}
