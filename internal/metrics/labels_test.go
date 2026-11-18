package metrics_test

import (
	"regexp"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"echpochmak/internal/api"
	"echpochmak/internal/catalog"
	"echpochmak/internal/fileio"
	"echpochmak/internal/kv/client"
	"echpochmak/internal/kv/node"
	"echpochmak/internal/kv/server"
	"echpochmak/internal/kv/transport"
	"echpochmak/internal/metrics"
	"echpochmak/internal/store/instrumented"
)

// allowedLabels are the only label names metrics may use. Every value set
// they take is bounded: never a table, namespace, key or request ID, which
// belong in traces and logs. Extend this list only with a bounded label.
func allowedLabels() map[string]bool {
	return map[string]bool{
		"route": true, "method": true, "status": true, "code": true,
		"kind": true, "outcome": true, "op": true, "error": true, "result": true,
		"stage": true, "peer": true, "follower": true, "node": true, "zone": true,
		"component": true, "version": true, "revision": true, "go_version": true,
	}
}

var labelList = regexp.MustCompile(`(?:variableLabels|constLabels): \{([^}]*)\}`)

func TestMetricLabelsAreBounded(t *testing.T) {
	t.Parallel()

	reg := metrics.NewRegistry("test")
	api.NewMetrics(reg)
	catalog.NewMetrics(reg)
	fileio.NewMetrics(reg)
	client.NewMetrics(reg)
	instrumented.NewMetrics(reg)
	node.NewMetrics(reg)
	transport.NewMetrics(reg)
	server.NewGRPCMetrics(reg)

	descs := make(chan *prometheus.Desc, 1000)
	reg.Describe(descs)
	close(descs)

	allowed := allowedLabels()
	n, labels := 0, 0

	for d := range descs {
		n++

		for _, m := range labelList.FindAllStringSubmatch(d.String(), -1) {
			for _, label := range strings.Split(m[1], ",") {
				name, _, _ := strings.Cut(strings.TrimSpace(label), "=")
				if name != "" {
					labels++
				}

				if name != "" && !allowed[name] {
					t.Errorf("%s uses label %q, which is not on the allow-list", d, name)
				}
			}
		}
	}

	if n < 30 || labels < 20 {
		t.Fatalf("%d metrics and %d labels described; the registry or the parsing is broken", n, labels)
	}
}
