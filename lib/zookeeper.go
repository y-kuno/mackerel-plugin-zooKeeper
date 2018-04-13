package mpzookeeper

import (
	"flag"

	"bufio"

	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"

	mp "github.com/mackerelio/go-mackerel-plugin"
)

// ZookeeperPlugin mackerel plugin
type ZookeeperPlugin struct {
	Host   string
	Port   string
	Prefix string
}

// MetricKeyPrefix interface for PluginWithPrefix
func (p *ZookeeperPlugin) MetricKeyPrefix() string {
	if p.Prefix == "" {
		p.Prefix = "zookeeper"
	}
	return p.Prefix
}

// GraphDefinition interface for mackerelplugin
func (p *ZookeeperPlugin) GraphDefinition() map[string]mp.Graphs {
	labelPrefix := strings.Title(p.Prefix)
	return map[string]mp.Graphs{
		"latency": {
			Label: labelPrefix + " Request Latency",
			Unit:  "integer",
			Metrics: []mp.Metrics{
				{Name: "zk_avg_latency", Label: "Average"},
				{Name: "zk_max_latency", Label: "Max"},
				{Name: "zk_min_latency", Label: "Min"},
			},
		},
		"packet": {
			Label: labelPrefix + " Packets",
			Unit:  "integer",
			Metrics: []mp.Metrics{
				{Name: "zk_packets_received", Label: "Received", Diff: true},
				{Name: "zk_packets_sent", Label: "Sent", Diff: true},
			},
		},
		"connection": {
			Label: labelPrefix + " Connections",
			Unit:  "integer",
			Metrics: []mp.Metrics{
				{Name: "zk_num_alive_connections", Label: "Connections"},
			},
		},
		"outstanding": {
			Label: labelPrefix + " Outstanding Requests",
			Unit:  "integer",
			Metrics: []mp.Metrics{
				{Name: "zk_outstanding_requests", Label: "Requests", Diff: true},
			},
		},
		"znode": {
			Label: labelPrefix + " Znodes",
			Unit:  "integer",
			Metrics: []mp.Metrics{
				{Name: "zk_znode_count", Label: "Znodes"},
			},
		},
		"watch": {
			Label: labelPrefix + " Watches",
			Unit:  "integer",
			Metrics: []mp.Metrics{
				{Name: "zk_watch_count", Label: "Watches"},
			},
		},
		"ephemerals": {
			Label: labelPrefix + " Ephemerals",
			Unit:  "integer",
			Metrics: []mp.Metrics{
				{Name: "zk_ephemerals_count", Label: "Ephemerals"},
			},
		},
		"data": {
			Label: labelPrefix + " Approximate Data Size",
			Unit:  "integer",
			Metrics: []mp.Metrics{
				{Name: "zk_approximate_data_size", Label: "Size"},
			},
		},
		"filedescriptor": {
			Label: labelPrefix + " File Descriptors",
			Unit:  "integer",
			Metrics: []mp.Metrics{
				{Name: "zk_open_file_descriptor_count", Label: "Open"},
				{Name: "zk_max_file_descriptor_count", Label: "Max"},
			},
		},
		"followers": {
			Label: labelPrefix + " Followers",
			Unit:  "integer",
			Metrics: []mp.Metrics{
				{Name: "zk_followers", Label: "Followers"},
				{Name: "zk_synced_followers", Label: "Synced Followers"},
				{Name: "zk_pending_syncs", Label: "Pending Syncs"},
			},
		},
	}
}

// FetchMetrics interface for mackerelplugin
func (p *ZookeeperPlugin) FetchMetrics() (map[string]float64, error) {

	command := fmt.Sprintf("echo mntr | nc %s %s", p.Host, p.Port)
	cmd := exec.Command("/bin/sh", "-c", command)
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	return p.parseMetrics(out)
}

func (p *ZookeeperPlugin) parseMetrics(out io.Reader) (map[string]float64, error) {
	metrics := make(map[string]float64)

	scanner := bufio.NewScanner(out)
	for scanner.Scan() {
		line := scanner.Text()
		record := strings.Fields(line)
		if record[0] == "zk_version" || record[0] == "zk_server_state" {
			continue
		}

		value, err := strconv.ParseFloat(string(record[1]), 64)
		if err != nil {
			return nil, err
		}
		metrics[record[0]] = value
	}
	return metrics, nil
}

// Do the plugin
func Do() {
	optHost := flag.String("host", "localhost", "Hostname")
	optPort := flag.String("port", "2185", "Port")
	optPrefix := flag.String("metric-key-prefix", "zookeeper", "Metric key prefix")
	optTempfile := flag.String("tempfile", "", "Temp file name")
	flag.Parse()

	plugin := mp.NewMackerelPlugin(&ZookeeperPlugin{
		Host:   *optHost,
		Port:   *optPort,
		Prefix: *optPrefix,
	})
	plugin.Tempfile = *optTempfile
	plugin.Run()
}
