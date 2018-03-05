package s3

import (
	"encoding/csv"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/stripe/veneur/samplers"
)

const PartitionDateFormat = "20060102"

type tsvField int

const (
	// the order in which these appear determines the
	// order of the fields in the resultant TSV
	TsvName tsvField = iota
	TsvTags
	TsvMetricType

	// The hostName of the server flushing the data
	TsvVeneurHostname

	TsvInterval

	TsvTimestamp
	TsvValue

	// This is the _partition field
	// required by the Redshift IncrementalLoader.
	// For our purposes, the current date is a good partition.
	TsvPartition
)

var tsvSchema = [...]string{
	TsvName:           "Name",
	TsvTags:           "Tags",
	TsvMetricType:     "MetricType",
	TsvInterval:       "Interval",
	TsvVeneurHostname: "VeneurHostname",
	TsvTimestamp:      "Timestamp",
	TsvValue:          "Value",
	TsvPartition:      "Partition",
}

// EncodeInterMetricCSV generates a newline-terminated CSV row that describes
// the data represented by the InterMetric.
// The caller is responsible for setting w.Comma as the appropriate delimiter.
// For performance, encodeCSV does not flush after every call; the caller is
// expected to flush at the end of the operation cycle
func EncodeInterMetricCSV(d samplers.InterMetric, w *csv.Writer, partitionDate *time.Time, hostName string, interval float64, timeFormat string, tags []string) error {
	// TODO(aditya) some better error handling for this
	// to guarantee that the result is proper JSON

	allTags := make([]string, len(tags), len(tags)+len(d.Tags))
	if len(tags) > 0 {
		copy(allTags, tags)
	}
	if len(d.Tags) > 0 {
		allTags = append(allTags, d.Tags...)
	}
	jsonTags := "{" + strings.Join(allTags, ",") + "}"

	metricType := ""
	metricValue := d.Value
	switch d.Type {
	case samplers.CounterMetric:
		metricValue = d.Value / interval
		metricType = "rate"

	case samplers.GaugeMetric:
		metricType = "gauge"
	default:
		return errors.New(fmt.Sprintf("Encountered an unknown metric type %s", d.Type.String()))
	}

	fields := [...]string{
		// the order here doesn't actually matter
		// as long as the keys are right
		TsvName:           d.Name,
		TsvTags:           jsonTags,
		TsvMetricType:     metricType,
		TsvInterval:       strconv.FormatFloat(interval, 'f', -1, 64),
		TsvVeneurHostname: hostName,
		TsvValue:          strconv.FormatFloat(metricValue, 'f', -1, 64),

		TsvTimestamp: time.Unix(d.Timestamp, 0).UTC().Format(timeFormat),

		// TODO avoid edge case at midnight
		TsvPartition: partitionDate.UTC().Format(PartitionDateFormat),
	}

	w.Write(fields[:])
	return w.Error()
}

// String returns the field Name.
// eg tsvName.String() returns "Name"
func (f tsvField) String() string {
	return fmt.Sprintf(strings.Replace(tsvSchema[f], "tsv", "", 1))
}

// each key in tsvMapping is guaranteed to have a unique value
var tsvMapping = map[string]int{}

func init() {
	for i, field := range tsvSchema {
		tsvMapping[field] = i
	}
}
