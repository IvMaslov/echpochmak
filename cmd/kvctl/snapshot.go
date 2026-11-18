package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"echpochmak/internal/config"
	"echpochmak/internal/kv/client"
	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/node"
	"echpochmak/internal/kv/snapshot"
	"echpochmak/internal/version"
)

func (cmd *command) snapshot(args []string) error {
	if len(args) == 0 {
		return errors.New("snapshot needs a subcommand: save, status or restore")
	}

	switch args[0] {
	case "save":
		return cmd.snapshotSave(args[1:])
	case "status":
		return cmd.snapshotStatus(args[1:])
	case "restore":
		return cmd.snapshotRestore(args[1:])
	default:
		return fmt.Errorf("unknown snapshot subcommand %q", args[0])
	}
}

// snapshotSave writes a snapshot to a file: from the cluster, preferring a
// follower, or offline from a stopped node's data directory. The file
// appears only once it is complete and verified.
func (cmd *command) snapshotSave(args []string) error {
	var endpoint, dataDir string

	pos, err := subFlags("snapshot save", args, 1, func(fs *flag.FlagSet) {
		fs.StringVar(&endpoint, "endpoint", "", "take the snapshot from this endpoint; default: a follower")
		fs.StringVar(&dataDir, "data-dir", "",
			"take the snapshot offline from this stopped node's data directory (unsafe recovery)")
	})
	if err != nil {
		return err
	}

	path := pos[0]
	tmp := path + ".partial"

	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("creating %s: %w", tmp, err)
	}

	// Gone after the rename; otherwise best effort.
	defer os.Remove(tmp)

	if dataDir != "" {
		err = saveOffline(f, dataDir)
	} else {
		err = cmd.saveOnline(f, endpoint)
	}

	if err == nil {
		err = f.Sync()
	}

	if closeErr := f.Close(); err == nil {
		err = closeErr
	}

	if err != nil {
		return err
	}

	info, err := verifyFile(tmp)
	if err != nil {
		return fmt.Errorf("the snapshot just written is invalid: %w", err)
	}

	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("renaming to %s: %w", path, err)
	}

	return cmd.printInfo(path, info)
}

func (cmd *command) saveOnline(w io.Writer, endpoint string) error {
	if endpoint == "" {
		endpoint = cmd.pickFollower()
	}

	if _, err := cmd.c.ExportSnapshot(cmd.ctx, endpoint, client.To(w)); err != nil {
		return err
	}

	return nil
}

// pickFollower returns the endpoint of a follower, to keep the export off the
// leader, or the first endpoint that answers.
func (cmd *command) pickFollower() string {
	statuses, _ := cmd.c.Status(cmd.ctx)
	fallback := ""

	for _, ep := range cmd.c.Endpoints() {
		st, ok := statuses[ep]
		if !ok {
			continue
		}

		if st.GetLeaderId() != 0 && st.GetLeaderId() != st.GetHeader().GetMemberId() {
			return ep
		}

		if fallback == "" {
			fallback = ep
		}
	}

	if fallback == "" {
		return cmd.c.Endpoints()[0]
	}

	return fallback
}

func saveOffline(w io.Writer, dataDir string) error {
	var body bytes.Buffer

	exported, err := node.ExportDataDir(nil, dataDir, &body)
	if err != nil {
		return err
	}

	sw, err := snapshot.NewWriter(w, &kvpb.SnapshotHeader{
		ClusterId:       exported.ClusterID,
		Index:           exported.State.GetIndex(),
		Term:            exported.State.GetTerm(),
		Revision:        exported.State.GetRevision(),
		Members:         exported.State.GetMembers(),
		CreatedUnixNano: time.Now().UnixNano(),
		Version:         version.Get().Version,
	})
	if err != nil {
		return err
	}

	if _, err := body.WriteTo(sw); err != nil {
		return fmt.Errorf("writing the snapshot: %w", err)
	}

	return sw.Close()
}

func (cmd *command) snapshotStatus(args []string) error {
	pos, err := subFlags("snapshot status", args, 1, nil)
	if err != nil {
		return err
	}

	info, err := verifyFile(pos[0])
	if err != nil {
		return err
	}

	return cmd.printInfo(pos[0], info)
}

// snapshotRestore creates one node's data directory of a new cluster from a
// snapshot file. Run it for every node of the new cluster with the same file
// and the same -initial-cluster, then start the nodes.
func (cmd *command) snapshotRestore(args []string) error {
	var (
		id             uint64
		dataDir        string
		initialCluster string
		bump           int64
	)

	pos, err := subFlags("snapshot restore", args, 1, func(fs *flag.FlagSet) {
		fs.Uint64Var(&id, "id", 0, "ID of the node to restore")
		fs.StringVar(&dataDir, "data-dir", "", "empty data directory of the node")
		fs.StringVar(&initialCluster, "initial-cluster", "", "voters of the new cluster: id=peer-url,...")
		fs.Int64Var(&bump, "revision-bump", node.DefaultRevisionBump,
			"how far the revision moves forward, above anything the old cluster issued")
	})
	if err != nil {
		return err
	}

	if dataDir == "" || initialCluster == "" {
		return errors.New("snapshot restore needs -id, -data-dir and -initial-cluster")
	}

	members, err := config.ParseInitialCluster(initialCluster)
	if err != nil {
		return err
	}

	f, err := os.Open(pos[0])
	if err != nil {
		return fmt.Errorf("opening the snapshot: %w", err)
	}
	defer f.Close()

	state, err := snapshot.Restore(f, node.RestoreConfig{
		ID:           id,
		DataDir:      filepath.Clean(dataDir),
		Members:      members,
		RevisionBump: bump,
	})
	if err != nil {
		return err
	}

	return cmd.printResult(map[string]any{
		"node":      id,
		"data_dir":  dataDir,
		"index":     state.GetIndex(),
		keyRevision: state.GetRevision(),
		"members":   len(state.GetMembers()),
	})
}

func verifyFile(path string) (snapshot.Info, error) {
	f, err := os.Open(path)
	if err != nil {
		return snapshot.Info{}, fmt.Errorf("opening the snapshot: %w", err)
	}
	defer f.Close()

	return snapshot.Verify(f)
}

func (cmd *command) printInfo(path string, info snapshot.Info) error {
	h := info.Header

	return cmd.printResult(map[string]any{
		"file":       path,
		"cluster_id": h.GetClusterId(),
		"node":       h.GetNodeId(),
		"index":      h.GetIndex(),
		"term":       h.GetTerm(),
		keyRevision:  h.GetRevision(),
		"members":    len(h.GetMembers()),
		"created":    time.Unix(0, h.GetCreatedUnixNano()).UTC().Format(time.RFC3339),
		"version":    h.GetVersion(),
		"sha256":     info.Checksum,
		"body_bytes": info.BodySize,
	})
}
