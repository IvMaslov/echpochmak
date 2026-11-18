// Command kvctl administers the KV cluster: it reads and writes keys,
// manages membership, and reports node status.
//
// Usage:
//
//	kvctl [-endpoints host:port,...] [-timeout 10s] [-json] COMMAND [ARGS]
//
// Commands:
//
//	get KEY [-prefix] [-serializable]   read a key, or every key with a prefix
//	put KEY VALUE                       write a key
//	del KEY [-prefix]                   delete a key, or every key with a prefix
//	member list                         list members
//	member add ID PEER_URL              add a node as a learner
//	member promote ID                   make a caught-up learner a voter
//	member remove ID                    remove a node
//	leader transfer [ID]                hand leadership to ID, or the best follower
//	status                              show the status of every endpoint
//	snapshot save FILE [-endpoint EP]   save a consistent snapshot, from a follower by default
//	snapshot save FILE -data-dir DIR    save a stopped node's state offline (unsafe recovery)
//	snapshot status FILE                verify a snapshot file and show its header
//	snapshot restore FILE -id ID -data-dir DIR -initial-cluster id=url,... [-revision-bump N]
//	                                    create one node of a new cluster from a snapshot
//
// The endpoints default to $ECHPOCHMAK_KV_ENDPOINTS, then localhost:2379.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"echpochmak/internal/kv/client"
)

const defaultEndpoints = "localhost:2379"

var errUsage = errors.New("usage")

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		if !errors.Is(err, errUsage) {
			fmt.Fprintf(os.Stderr, "kvctl: %v\n", err)
		}

		os.Exit(1)
	}
}

// options are the global flags.
type options struct {
	endpoints string
	timeout   time.Duration
	json      bool
}

func run(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("kvctl", flag.ContinueOnError)

	var opts options

	endpoints := os.Getenv("ECHPOCHMAK_KV_ENDPOINTS")
	if endpoints == "" {
		endpoints = defaultEndpoints
	}

	fs.StringVar(&opts.endpoints, "endpoints", endpoints, "client endpoints of the cluster, comma-separated")
	fs.DurationVar(&opts.timeout, "timeout", 10*time.Second, "timeout of the whole command")
	fs.BoolVar(&opts.json, "json", false, "print JSON instead of a table")

	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: kvctl [flags] COMMAND [ARGS]; see the package documentation for commands.")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return errUsage
	}

	if fs.NArg() == 0 {
		fs.Usage()

		return errUsage
	}

	c, err := client.New(client.Config{
		Endpoints: strings.Split(opts.endpoints, ","),
		Logger:    slog.New(slog.DiscardHandler),
	})
	if err != nil {
		return err
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
	defer cancel()

	cmd := &command{ctx: ctx, c: c, out: out, json: opts.json}

	return cmd.dispatch(fs.Args())
}
