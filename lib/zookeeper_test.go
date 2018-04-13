package mpzookeeper

import (
	"testing"

	"bytes"

	"github.com/stretchr/testify/assert"
)

func TestParseMetrics(t *testing.T) {
	str := `zk_version	3.4.6-1569965, built on 02/20/2014 09:09 GMT
zk_avg_latency	0
zk_max_latency	1106
zk_min_latency	0
zk_packets_received	704340142
zk_packets_sent	704347929
zk_num_alive_connections	16
zk_outstanding_requests	0
zk_server_state	leader
zk_znode_count	1475
zk_watch_count	88
zk_ephemerals_count	113
zk_approximate_data_size	1337130
zk_open_file_descriptor_count	45
zk_max_file_descriptor_count	16384
zk_followers	2
zk_synced_followers	2
zk_pending_syncs	0`

	var p ZookeeperPlugin
	metrics, err := p.parseMetrics(bytes.NewBufferString(str))
	if err != nil {
		t.Fatal(err)
	}

	assert.EqualValues(t, metrics["zk_avg_latency"], 0)
	assert.EqualValues(t, metrics["zk_max_latency"], 1106)
	assert.EqualValues(t, metrics["zk_min_latency"], 0)
	assert.EqualValues(t, metrics["zk_packets_received"], 704340142)
	assert.EqualValues(t, metrics["zk_packets_sent"], 704347929)
	assert.EqualValues(t, metrics["zk_num_alive_connections"], 16)
	assert.EqualValues(t, metrics["zk_outstanding_requests"], 0)
	assert.EqualValues(t, metrics["zk_znode_count"], 1475)
	assert.EqualValues(t, metrics["zk_watch_count"], 88)
	assert.EqualValues(t, metrics["zk_ephemerals_count"], 113)
	assert.EqualValues(t, metrics["zk_approximate_data_size"], 1337130)
	assert.EqualValues(t, metrics["zk_open_file_descriptor_count"], 45)
	assert.EqualValues(t, metrics["zk_max_file_descriptor_count"], 16384)
	assert.EqualValues(t, metrics["zk_followers"], 2)
	assert.EqualValues(t, metrics["zk_synced_followers"], 2)
	assert.EqualValues(t, metrics["zk_pending_syncs"], 0)
}
