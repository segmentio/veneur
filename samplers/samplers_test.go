package samplers

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCounterEmpty(t *testing.T) {

	c := NewCounter("a.b.c", []string{"a:b"})
	c.Sample(1, 1.0)

	assert.Equal(t, "a.b.c", c.Name, "Name")
	assert.Len(t, c.Tags, 1, "Tag length")
	assert.Equal(t, c.Tags[0], "a:b", "Tag contents")

	metrics := c.Flush(10 * time.Second)
	assert.Len(t, metrics, 1, "Flushes 1 metric")

	m1 := metrics[0]
	assert.Equal(t, int32(10), m1.Interval, "Interval")
	assert.Equal(t, "rate", m1.MetricType, "Type")
	assert.Len(t, c.Tags, 1, "Tag length")
	assert.Equal(t, c.Tags[0], "a:b", "Tag contents")
	// The counter returns an array with a single tuple of timestamp,value
	assert.Equal(t, 0.1, m1.Value[0][1], "Metric value")
}

func TestCounterRate(t *testing.T) {

	c := NewCounter("a.b.c", []string{"a:b"})

	c.Sample(5, 1.0)

	// The counter returns an array with a single tuple of timestamp,value
	metrics := c.Flush(10 * time.Second)
	assert.Equal(t, 0.5, metrics[0].Value[0][1], "Metric value")
}

func TestCounterSampleRate(t *testing.T) {

	c := NewCounter("a.b.c", []string{"a:b"})

	c.Sample(5, 0.5)

	// The counter returns an array with a single tuple of timestamp,value
	metrics := c.Flush(10 * time.Second)
	assert.Equal(t, float64(1), metrics[0].Value[0][1], "Metric value")
}

func TestGauge(t *testing.T) {

	g := NewGauge("a.b.c", []string{"a:b"})

	assert.Equal(t, "a.b.c", g.Name, "Name")
	assert.Len(t, g.Tags, 1, "Tag length")
	assert.Equal(t, g.Tags[0], "a:b", "Tag contents")

	g.Sample(5, 1.0)

	metrics := g.Flush()
	assert.Len(t, metrics, 1, "Flushed metric count")

	m1 := metrics[0]
	// Interval is not meaningful for this
	assert.Equal(t, int32(0), m1.Interval, "Interval")
	assert.Equal(t, "gauge", m1.MetricType, "Type")
	tags := m1.Tags
	assert.Len(t, tags, 1, "Tag length")
	assert.Equal(t, tags[0], "a:b", "Tag contents")

	// The counter returns an array with a single tuple of timestamp,value
	assert.Equal(t, float64(5), m1.Value[0][1], "Value")
}

func TestSet(t *testing.T) {
	s := NewSet("a.b.c", []string{"a:b"})

	assert.Equal(t, "a.b.c", s.Name, "Name")
	assert.Len(t, s.Tags, 1, "Tag count")
	assert.Equal(t, "a:b", s.Tags[0], "First tag")

	s.Sample("5", 1.0)

	s.Sample("5", 1.0)

	s.Sample("123", 1.0)

	s.Sample("2147483647", 1.0)
	s.Sample("-2147483648", 1.0)

	metrics := s.Flush()
	assert.Len(t, metrics, 1, "Flush")

	m1 := metrics[0]
	// Interval is not meaningful for this
	assert.Equal(t, int32(0), m1.Interval, "Interval")
	assert.Equal(t, "gauge", m1.MetricType, "Type")
	assert.Len(t, m1.Tags, 1, "Tag count")
	assert.Equal(t, "a:b", m1.Tags[0], "First tag")
	assert.Equal(t, float64(4), m1.Value[0][1], "Value")
}

func TestSetMerge(t *testing.T) {
	rand.Seed(time.Now().Unix())

	s := NewSet("a.b.c", []string{"a:b"})
	for i := 0; i < 100; i++ {
		s.Sample(strconv.Itoa(rand.Int()), 1.0)
	}
	assert.Equal(t, uint64(100), s.Hll.Count(), "counts did not match")

	jm, err := s.Export()
	assert.NoError(t, err, "should have exported successfully")

	s2 := NewSet("a.b.c", []string{"a:b"})
	assert.NoError(t, s2.Combine(jm.Value), "should have combined successfully")
	// HLLs are approximate, and we've seen error of +-1 here in the past, so
	// we're giving the test some room for error to reduce flakes
	count1 := int(s.Hll.Count())
	count2 := int(s2.Hll.Count())
	countDifference := count1 - count2
	assert.True(t, -1 <= countDifference && countDifference <= 1, "counts did not match after merging (%d and %d)", count1, count2)
}

func TestHisto(t *testing.T) {

	h := NewHist("a.b.c", []string{"a:b"})

	assert.Equal(t, "a.b.c", h.Name, "Name")
	assert.Len(t, h.Tags, 1, "Tag count")
	assert.Equal(t, "a:b", h.Tags[0], "First tag")

	h.Sample(5, 1.0)
	h.Sample(10, 1.0)
	h.Sample(15, 1.0)
	h.Sample(20, 1.0)
	h.Sample(25, 1.0)

	var aggregates HistogramAggregates
	aggregates.Value = AggregateMin | AggregateMax | AggregateMedian |
		AggregateAverage | AggregateCount | AggregateSum
	aggregates.Count = 6

	percentiles := []float64{0.90}

	metrics := h.Flush(10*time.Second, percentiles, aggregates)
	// We get lots of metrics back for histograms!
	// One for each of the aggregates specified, plus
	// one for the explicit percentile we are asking for
	assert.Len(t, metrics, aggregates.Count+len(percentiles), "Flushed metrics length")

	// the max
	m2 := metrics[0]
	assert.Equal(t, "a.b.c.max", m2.Name, "Name")
	assert.Equal(t, int32(0), m2.Interval, "Interval")
	assert.Equal(t, "gauge", m2.MetricType, "Type")
	assert.Len(t, m2.Tags, 1, "Tag count")
	assert.Equal(t, "a:b", m2.Tags[0], "First tag")
	// The counter returns an array with a single tuple of timestamp,value
	assert.Equal(t, float64(25), m2.Value[0][1], "Value")

	// the min
	m3 := metrics[1]
	assert.Equal(t, "a.b.c.min", m3.Name, "Name")
	assert.Equal(t, int32(0), m3.Interval, "Interval")
	assert.Equal(t, "gauge", m3.MetricType, "Type")
	assert.Len(t, m3.Tags, 1, "Tag count")
	assert.Equal(t, "a:b", m3.Tags[0], "First tag")
	// The counter returns an array with a single tuple of timestamp,value
	assert.Equal(t, float64(5), m3.Value[0][1], "Value")

	// the sum
	m4 := metrics[2]
	assert.Equal(t, "a.b.c.sum", m4.Name, "Name")
	assert.Equal(t, int32(0), m4.Interval, "Interval")
	assert.Equal(t, "gauge", m4.MetricType, "Type")
	assert.Len(t, m4.Tags, 1, "Tag count")
	assert.Equal(t, "a:b", m4.Tags[0], "First tag")
	// The counter returns an array with a single tuple of timestamp,value
	assert.Equal(t, float64(75), m4.Value[0][1], "Value")

	// the average
	m5 := metrics[3]
	assert.Equal(t, "a.b.c.avg", m5.Name, "Name")
	assert.Equal(t, int32(0), m5.Interval, "Interval")
	assert.Equal(t, "gauge", m5.MetricType, "Type")
	assert.Len(t, m5.Tags, 1, "Tag count")
	assert.Equal(t, "a:b", m5.Tags[0], "First tag")
	// The counter returns an array with a single tuple of timestamp,value
	assert.Equal(t, float64(15), m5.Value[0][1], "Value")

	// the count
	m1 := metrics[4]
	assert.Equal(t, "a.b.c.count", m1.Name, "Name")
	assert.Equal(t, int32(10), m1.Interval, "Interval")
	assert.Equal(t, "rate", m1.MetricType, "Type")
	assert.Len(t, m1.Tags, 1, "Tag count")
	assert.Equal(t, "a:b", m1.Tags[0], "First tag")
	// The counter returns an array with a single tuple of timestamp,value
	assert.Equal(t, float64(0.5), m1.Value[0][1], "Value")

	// the median
	m6 := metrics[5]
	assert.Equal(t, "a.b.c.median", m6.Name, "Name")
	assert.Equal(t, int32(0), m6.Interval, "Interval")
	assert.Equal(t, "gauge", m6.MetricType, "Type")
	assert.Len(t, m6.Tags, 1, "Tag count")
	assert.Equal(t, "a:b", m6.Tags[0], "First tag")
	// The counter returns an array with a single tuple of timestamp,value
	assert.Equal(t, float64(15), m6.Value[0][1], "Value")

	// And the percentile
	m7 := metrics[6]
	assert.Equal(t, "a.b.c.90percentile", m7.Name, "Name")
	assert.Equal(t, int32(0), m7.Interval, "Interval")
	assert.Equal(t, "gauge", m7.MetricType, "Type")
	assert.Len(t, m7.Tags, 1, "Tag count")
	assert.Equal(t, "a:b", m7.Tags[0], "First tag")
	// The counter returns an array with a single tuple of timestamp,value
	assert.Equal(t, float64(23.75), m7.Value[0][1], "Value")
}

func TestHistoSampleRate(t *testing.T) {

	h := NewHist("a.b.c", []string{"a:b"})

	assert.Equal(t, "a.b.c", h.Name, "Name")
	assert.Len(t, h.Tags, 1, "Tag length")
	assert.Equal(t, h.Tags[0], "a:b", "Tag contents")

	h.Sample(5, 0.5)
	h.Sample(10, 0.5)
	h.Sample(15, 0.5)
	h.Sample(20, 0.5)
	h.Sample(25, 0.5)

	var aggregates HistogramAggregates
	aggregates.Value = AggregateMin | AggregateMax | AggregateCount
	aggregates.Count = 3

	metrics := h.Flush(10*time.Second, []float64{0.50}, aggregates)
	assert.Len(t, metrics, 4, "Metrics flush length")

	// First the max
	m1 := metrics[0]
	assert.Equal(t, "a.b.c.max", m1.Name, "Max name")
	assert.Equal(t, float64(25), m1.Value[0][1], "Sampled max as rate")

	count := metrics[2]
	assert.Equal(t, "a.b.c.count", count.Name, "count name")
	assert.Equal(t, float64(1), count.Value[0][1], "count value")
}

func TestHistoMerge(t *testing.T) {
	rand.Seed(time.Now().Unix())

	h := NewHist("a.b.c", []string{"a:b"})
	for i := 0; i < 100; i++ {
		h.Sample(rand.NormFloat64(), 1.0)
	}

	jm, err := h.Export()
	assert.NoError(t, err, "should have exported successfully")

	h2 := NewHist("a.b.c", []string{"a:b"})
	assert.NoError(t, h2.Combine(jm.Value), "should have combined successfully")
	assert.InEpsilon(t, h.Value.Quantile(0.5), h2.Value.Quantile(0.5), 0.02, "50th percentiles did not match after merging")
	assert.InDelta(t, 0, h2.LocalWeight, 0.02, "merged histogram should have count of zero")
	assert.True(t, math.IsInf(h2.LocalMin, +1), "merged histogram should have local minimum of +inf")
	assert.True(t, math.IsInf(h2.LocalMax, -1), "merged histogram should have local minimum of -inf")

	h2.Sample(1.0, 1.0)
	assert.InDelta(t, 1.0, h2.LocalWeight, 0.02, "merged histogram should have count of 1 after adding a value")
	assert.InDelta(t, 1.0, h2.LocalMin, 0.02, "merged histogram should have min of 1 after adding a value")
	assert.InDelta(t, 1.0, h2.LocalMax, 0.02, "merged histogram should have max of 1 after adding a value")
}

type CSVTestCase struct {
	Name     string
	DDMetric DDMetric
	Row      io.Reader
}

func CSVTestCases() []CSVTestCase {

	partition := time.Now().UTC().Format("20060102")

	return []CSVTestCase{
		{
			Name: "BasicDDMetric",
			DDMetric: DDMetric{
				Name: "a.b.c.max",
				Value: [1][2]float64{[2]float64{1476119058,
					100}},
				Tags: []string{"foo:bar",
					"baz:quz"},
				MetricType: "gauge",
				Hostname:   "globalstats",
				DeviceName: "food",
				Interval:   0,
			},
			Row: strings.NewReader(fmt.Sprintf("a.b.c.max\t{foo:bar,baz:quz}\tgauge\tglobalstats\ttestbox-c3eac9\tfood\t0\t2016-10-10 05:04:18\t100\t%s\n", partition)),
		},
		{
			// Test that we are able to handle a missing field (DeviceName)
			Name: "MissingDeviceName",
			DDMetric: DDMetric{
				Name: "a.b.c.max",
				Value: [1][2]float64{[2]float64{1476119058,
					100}},
				Tags: []string{"foo:bar",
					"baz:quz"},
				MetricType: "rate",
				Hostname:   "localhost",
				DeviceName: "",
				Interval:   10,
			},
			Row: strings.NewReader(fmt.Sprintf("a.b.c.max\t{foo:bar,baz:quz}\trate\tlocalhost\ttestbox-c3eac9\t\t10\t2016-10-10 05:04:18\t100\t%s\n", partition)),
		},
		{
			// Test that we are able to handle tags which have tab characters in them
			// by quoting the entire field
			// (tags shouldn't do this, but we should handle them properly anyway)
			Name: "TabTag",
			DDMetric: DDMetric{
				Name: "a.b.c.max",
				Value: [1][2]float64{[2]float64{1476119058,
					100}},
				Tags: []string{"foo:b\tar",
					"baz:quz"},
				MetricType: "rate",
				Hostname:   "localhost",
				DeviceName: "eniac",
				Interval:   10,
			},
			Row: strings.NewReader(fmt.Sprintf("a.b.c.max\t\"{foo:b\tar,baz:quz}\"\trate\tlocalhost\ttestbox-c3eac9\teniac\t10\t2016-10-10 05:04:18\t100\t%s\n", partition)),
		},
	}
}

func TestEncodeCSV(t *testing.T) {
	testCases := CSVTestCases()

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {

			b := &bytes.Buffer{}

			w := csv.NewWriter(b)
			w.Comma = '\t'

			tm := time.Now()
			err := tc.DDMetric.EncodeCSV(w, &tm, "testbox-c3eac9")
			assert.NoError(t, err)

			// We need to flush or there won't actually be any data there
			w.Flush()
			assert.NoError(t, err)

			assertReadersEqual(t, tc.Row, b)
		})
	}
}

// Helper function for determining that two readers are equal
func assertReadersEqual(t *testing.T, expected io.Reader, actual io.Reader) {

	// If we can seek, ensure that we're starting at the beginning
	for _, reader := range []io.Reader{expected, actual} {
		if readerSeeker, ok := reader.(io.ReadSeeker); ok {
			readerSeeker.Seek(0, io.SeekStart)
		}
	}

	// do the lazy thing for now
	bts, err := ioutil.ReadAll(expected)
	if err != nil {
		t.Fatal(err)
	}

	bts2, err := ioutil.ReadAll(actual)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, string(bts), string(bts2))
}