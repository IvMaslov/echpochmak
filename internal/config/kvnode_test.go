package config_test

import (
	"strings"
	"testing"

	"echpochmak/internal/config"
)

func TestLoadKVNode(t *testing.T) {
	t.Parallel()

	vars := map[string]string{
		"ECHPOCHMAK_KV_DATA_DIR": "/var/lib/kv",
		"ECHPOCHMAK_KV_JOIN":     "a:2379, b:2379",
	}

	cfg, err := config.LoadKVNode([]string{"-id", "3"}, env(vars))
	if err != nil {
		t.Fatalf("LoadKVNode: %v", err)
	}

	if cfg.ID != 3 || cfg.DataDir != "/var/lib/kv" || len(cfg.Join) != 2 || cfg.Join[1] != "b:2379" {
		t.Fatalf("cfg = %+v", cfg)
	}
}

func TestLoadKVNodeErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "no id", args: []string{"-data-dir", "/d"}, want: "id is required"},
		{name: "no data dir", args: []string{"-id", "1"}, want: "data-dir"},
		{
			name: "bootstrap and join",
			args: []string{"-id", "1", "-data-dir", "/d", "-initial-cluster", "1=http://a:2380", "-join", "b:2379"},
			want: "mutually exclusive",
		},
		{
			name: "bad cluster",
			args: []string{"-id", "1", "-data-dir", "/d", "-initial-cluster", "x"},
			want: "id=peer-url",
		},
		{
			name: "election shorter than heartbeat",
			args: []string{"-id", "1", "-data-dir", "/d", "-election-ticks", "1"},
			want: "election-ticks",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := config.LoadKVNode(tt.args, env(nil))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want one mentioning %q", err, tt.want)
			}
		})
	}
}

func TestParseInitialCluster(t *testing.T) {
	t.Parallel()

	members, err := config.ParseInitialCluster("1=http://a:2380, 2=http://b:2380")
	if err != nil || len(members) != 2 || members[1].GetId() != 2 || members[1].GetPeerUrl() != "http://b:2380" {
		t.Fatalf("ParseInitialCluster = %v, %v", members, err)
	}

	for _, bad := range []string{"1=a,1=b", "0=a", "a=b", "1="} {
		if _, err := config.ParseInitialCluster(bad); err == nil {
			t.Errorf("ParseInitialCluster(%q) succeeded", bad)
		}
	}
}

func TestKVEnvName(t *testing.T) {
	t.Parallel()

	if got := config.KVEnvName("initial-cluster"); got != "ECHPOCHMAK_KV_INITIAL_CLUSTER" {
		t.Fatalf("KVEnvName = %q", got)
	}
}
