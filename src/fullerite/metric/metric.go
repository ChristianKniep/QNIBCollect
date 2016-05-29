package metric

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

// The different types of metrics that are supported
const (
	Gauge             = "gauge"
	Counter           = "counter"
	CumulativeCounter = "cumcounter"
	MaxGauge          = "gauge_max"
	MinGauge          = "gauge_min"
	Time              = "time"
)

// Metric type holds all the information for a single metric data
// point. Metrics are generated in collectors and passed to handlers.
// AggEnabled: If true handler should downsample the list of metrics to a single metric
type Metric struct {
	Name       string            `json:"name"`
	MetricType string            `json:"type"`
	Value      float64           `json:"value"`
	Dimensions map[string]string `json:"dimensions"`
	Buffered   bool              `json:"buffered"`
	Time       time.Time         `json:"time"`
	AggEnabled bool              `json:"agg-enabled"`
}

// AggregateList metrics of the same Name, Type and Dimension according
// TODO: Should be done by the buffer instead each time when AggregateList is called
func AggregateList(ms []Metric) []Metric {
	var res []Metric
	var cache map[string][]Metric
	var mid string
	// iterate through metrics and create a filter if no filter present
	for _, m := range ms {
		mid = m.GetFilterID()
		_, ok := cache[mid]
		if !ok {
			cache[mid] = []Metric{}
		}
		cache[mid] = append(cache[mid], m)
	}

	for _, ms := range cache {
		fm := ms[0]
		var sum, avg float64
		if fm.MetricType == "gauge" {
			sum = 0
			for _, m := range ms {
				sum += m.Value
			}
			avg = sum / float64(len(ms))
			nm := NewExt(fm.Name, fm.MetricType, avg, fm.Dimensions, fm.Time, false)
			res = append(res, nm)
		}
	}
	return res
}

// GetFilterID provides an identifier for a given metric
func (m *Metric) GetFilterID() string {
	res := fmt.Sprintf("%s_%s", m.Name, m.MetricType)
	dims := []string{}
	for k := range m.Dimensions {
		dims = append(dims, k)
	}
	sort.Strings(dims)
	for _, d := range dims {
		res = res + fmt.Sprintf("_%s:%s", d, m.Dimensions[d])
	}
	return res

}

// New returns a new metric with name. Default metric type is "gauge"
// and timestamp is set to now. Value is initialized to 0.0.
func New(name string) Metric {
	return Metric{
		Name:       sanitizeString(name),
		MetricType: "gauge",
		Value:      0.0,
		Dimensions: make(map[string]string),
		Time:       time.Now(),
		Buffered:   false,
		AggEnabled: false,
	}
}

// NewExt provides a more controled creation
func NewExt(name string, typ string, val float64, d map[string]string, t time.Time, b bool) Metric {
	return Metric{
		Name:       sanitizeString(name),
		MetricType: typ,
		Value:      val,
		Dimensions: d,
		Time:       t,
		Buffered:   b,
		AggEnabled: false,
	}
}

// Filter provides a struct that can filter a metric by Name (regex), type, dimension (subset of Dimensions)
type Filter struct {
	Name       string            `json:"name"`
	MetricType string            `json:"type"`
	Dimensions map[string]string `json:"dimensions"`
}

// ToJSON Transforms Filter to JSON
func (f *Filter) ToJSON() string {
	b, err := json.Marshal(f)
	if err != nil {
		fmt.Println(err)
		return "{}"
	}
	return string(b)
}

// NewFilter returns a Filter with compiled regex
func NewFilter(name string, t string, d map[string]string) Filter {
	return Filter{
		Name:       name,
		MetricType: t,
		Dimensions: d,
	}
}

// WithValue returns metric with value of type Gauge
func WithValue(name string, value float64) Metric {
	metric := New(name)
	metric.Value = value
	return metric
}

// EnableBuffering puts the metric into buffering handlers (e.g. ZmqBUF)
func (m *Metric) EnableBuffering() {
	m.Buffered = true
}

// DisableBuffering takes the metric out of buffering (e.g. ZmqBUF)
func (m *Metric) DisableBuffering() {
	m.Buffered = false
}

// SetTime to metric
func (m *Metric) SetTime(mtime time.Time) {
	m.Time = mtime
}

// GetTime returns the time
func (m *Metric) GetTime() time.Time {
	return m.Time
}

// AddDimension adds a new dimension to the Metric.
func (m *Metric) AddDimension(name, value string) {
	m.Dimensions[sanitizeString(name)] = sanitizeString(value)
}

// RemoveDimension removes a dimension from the Metric.
func (m *Metric) RemoveDimension(name string) {
	delete(m.Dimensions, name)
}

// AddDimensions adds multiple new dimensions to the Metric.
func (m *Metric) AddDimensions(dimensions map[string]string) {
	for k, v := range dimensions {
		m.AddDimension(k, v)
	}
}

// GetDimensions returns the dimensions of a metric merged with defaults. Defaults win.
func (m *Metric) GetDimensions(defaults map[string]string) (dimensions map[string]string) {
	dimensions = make(map[string]string)
	for name, value := range m.Dimensions {
		dimensions[name] = value
	}
	for name, value := range defaults {
		dimensions[name] = value
	}
	return dimensions
}

// GetDimensionValue returns the value of a dimension if it's set.
func (m *Metric) GetDimensionValue(dimension string) (value string, ok bool) {
	dimension = sanitizeString(dimension)
	value, ok = m.Dimensions[dimension]
	return
}

// ZeroValue is metric zero value
func (m *Metric) ZeroValue() bool {
	return (len(m.Name) == 0) &&
		(len(m.MetricType) == 0) &&
		(m.Value == 0.0) &&
		(len(m.Dimensions) == 0)
}

// AddToAll adds a map of dimensions to a list of metrics
func AddToAll(metrics *[]Metric, dims map[string]string) {
	for _, m := range *metrics {
		for key, value := range dims {
			m.AddDimension(key, value)
		}
	}
}

func sanitizeString(s string) string {
	s = strings.Replace(s, "=", "-", -1)
	s = strings.Replace(s, ":", "-", -1)
	return s
}

// ToJSON Transforms metric to JSON
func (m *Metric) ToJSON() string {
	b, err := json.Marshal(m)
	if err != nil {
		fmt.Println(err)
		return "{}"
	}
	return string(b)
}

// IsSubDim checks if the 2st map contains all items in the second
func (m *Metric) IsSubDim(other map[string]string) bool {
	for k, v := range other {
		val, ok := m.Dimensions[k]
		if !ok || v != val {
			return false
		}
	}
	return true
}

// IsFiltered checks if metrics is filtered with a given filter
func (m *Metric) IsFiltered(f Filter) bool {
	if !m.IsSubDim(f.Dimensions) {
		return false
	}
	if m.MetricType != f.MetricType {
		return false
	}
	// TODO: Precompile regex to speed up matching
	if !regexp.MustCompile(f.Name).MatchString(m.Name) {
		return false
	}

	return true
}
