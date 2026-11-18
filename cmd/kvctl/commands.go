package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"text/tabwriter"

	"echpochmak/internal/kv/client"
	"echpochmak/internal/kv/kvpb"
)

// keyRevision names the revision in command output.
const keyRevision = "revision"

type command struct {
	ctx  context.Context //nolint:containedctx // one command, one context.
	c    *client.Client
	out  io.Writer
	json bool
}

func (cmd *command) dispatch(args []string) error {
	name, rest := args[0], args[1:]

	switch name {
	case "get":
		return cmd.get(rest)
	case "put":
		return cmd.put(rest)
	case "del":
		return cmd.del(rest)
	case "member":
		return cmd.member(rest)
	case "leader":
		return cmd.leader(rest)
	case "status":
		return cmd.status()
	case "snapshot":
		return cmd.snapshot(rest)
	default:
		return fmt.Errorf("unknown command %q", name)
	}
}

// subFlags parses the flags of a subcommand and returns its positional
// arguments, which must number exactly want.
func subFlags(name string, args []string, want int, define func(*flag.FlagSet)) ([]string, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	if define != nil {
		define(fs)
	}

	// The flag package stops at the first positional argument; parse again
	// after each one, so flags may come before or after them.
	pos := []string{}

	for {
		if err := fs.Parse(args); err != nil {
			return nil, errUsage
		}

		if fs.NArg() == 0 {
			break
		}

		pos = append(pos, fs.Arg(0))
		args = fs.Args()[1:]
	}

	if len(pos) != want {
		return nil, fmt.Errorf("%s takes %d argument(s), got %d", name, want, len(pos))
	}

	return pos, nil
}

type kvRow struct {
	Key            string `json:"key"`
	Value          string `json:"value"`
	CreateRevision int64  `json:"create_revision"`
	ModRevision    int64  `json:"mod_revision"`
	Version        int64  `json:"version"`
}

func (cmd *command) get(args []string) error {
	var prefix, serializable bool

	pos, err := subFlags("get", args, 1, func(fs *flag.FlagSet) {
		fs.BoolVar(&prefix, "prefix", false, "read every key with this prefix")
		fs.BoolVar(&serializable, "serializable", false, "read the local state of the node, which may be stale")
	})
	if err != nil {
		return err
	}

	req := &kvpb.RangeRequest{Key: []byte(pos[0]), Serializable: serializable}
	if prefix {
		req.RangeEnd = client.PrefixEnd(req.GetKey())
	}

	resp, err := cmd.c.Range(cmd.ctx, req)
	if err != nil {
		return err
	}

	rows := make([]kvRow, 0, len(resp.GetKvs()))
	for _, kv := range resp.GetKvs() {
		rows = append(rows, kvRow{
			Key:            string(kv.GetKey()),
			Value:          string(kv.GetValue()),
			CreateRevision: kv.GetCreateRevision(),
			ModRevision:    kv.GetModRevision(),
			Version:        kv.GetVersion(),
		})
	}

	return printTable(cmd, rows, []string{"KEY", "VALUE", "MOD_REVISION", "VERSION"}, func(r kvRow) []string {
		return []string{r.Key, r.Value, strconv.FormatInt(r.ModRevision, 10), strconv.FormatInt(r.Version, 10)}
	})
}

func (cmd *command) put(args []string) error {
	pos, err := subFlags("put", args, 2, nil)
	if err != nil {
		return err
	}

	resp, err := cmd.c.Put(cmd.ctx, []byte(pos[0]), []byte(pos[1]))
	if err != nil {
		return err
	}

	return cmd.printResult(map[string]any{keyRevision: resp.GetHeader().GetRevision()})
}

func (cmd *command) del(args []string) error {
	var prefix bool

	pos, err := subFlags("del", args, 1, func(fs *flag.FlagSet) {
		fs.BoolVar(&prefix, "prefix", false, "delete every key with this prefix")
	})
	if err != nil {
		return err
	}

	var end []byte
	if prefix {
		end = client.PrefixEnd([]byte(pos[0]))
	}

	resp, err := cmd.c.Delete(cmd.ctx, []byte(pos[0]), end)
	if err != nil {
		return err
	}

	return cmd.printResult(map[string]any{"deleted": resp.GetDeleted(), keyRevision: resp.GetHeader().GetRevision()})
}

type memberRow struct {
	ID      uint64 `json:"id"`
	PeerURL string `json:"peer_url"`
	Learner bool   `json:"learner"`
}

func (cmd *command) member(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("member needs a subcommand: list, add, promote or remove: %w", errUsage)
	}

	members, err := cmd.memberCall(args[0], args[1:])
	if err != nil {
		return err
	}

	rows := make([]memberRow, 0, len(members))
	for _, m := range members {
		rows = append(rows, memberRow{ID: m.GetId(), PeerURL: m.GetPeerUrl(), Learner: m.GetLearner()})
	}

	return printTable(cmd, rows, []string{"ID", "PEER_URL", "ROLE"}, func(r memberRow) []string {
		role := "voter"
		if r.Learner {
			role = "learner"
		}

		return []string{strconv.FormatUint(r.ID, 10), r.PeerURL, role}
	})
}

// memberCall runs a member subcommand and returns the members afterwards.
func (cmd *command) memberCall(sub string, args []string) ([]*kvpb.Member, error) {
	if sub == "list" {
		resp, err := cmd.c.MemberList(cmd.ctx)

		return resp.GetMembers(), err
	}

	want := map[string]int{"add": 2, "promote": 1, "remove": 1}[sub]
	if want == 0 {
		return nil, fmt.Errorf("unknown member subcommand %q", sub)
	}

	pos, err := subFlags("member "+sub, args, want, nil)
	if err != nil {
		return nil, err
	}

	id, err := parseID(pos[0])
	if err != nil {
		return nil, err
	}

	switch sub {
	case "add":
		return cmd.c.MemberAdd(cmd.ctx, id, pos[1])
	case "promote":
		return cmd.c.MemberPromote(cmd.ctx, id)
	default:
		return cmd.c.MemberRemove(cmd.ctx, id)
	}
}

func (cmd *command) leader(args []string) error {
	if len(args) == 0 || args[0] != "transfer" || len(args) > 2 {
		return fmt.Errorf("usage: leader transfer [ID]: %w", errUsage)
	}

	var target uint64

	if len(args) == 2 {
		var err error
		if target, err = parseID(args[1]); err != nil {
			return err
		}
	}

	leader, err := cmd.c.LeaderTransfer(cmd.ctx, target)
	if err != nil {
		return err
	}

	return cmd.printResult(map[string]any{"leader": leader})
}

type statusRow struct {
	Endpoint string `json:"endpoint"`
	ID       uint64 `json:"id,omitempty"`
	Leader   uint64 `json:"leader,omitempty"`
	Zone     string `json:"zone,omitempty"`
	State    string `json:"state,omitempty"`
	Term     uint64 `json:"term,omitempty"`
	Commit   uint64 `json:"commit,omitempty"`
	Applied  uint64 `json:"applied,omitempty"`
	Error    string `json:"error,omitempty"`
}

func (cmd *command) status() error {
	statuses, errs := cmd.c.Status(cmd.ctx)
	rows := make([]statusRow, 0, len(cmd.c.Endpoints()))

	for _, ep := range cmd.c.Endpoints() {
		if err, ok := errs[ep]; ok {
			rows = append(rows, statusRow{Endpoint: ep, Error: err.Error()})

			continue
		}

		st := statuses[ep]
		rows = append(rows, statusRow{
			Endpoint: ep,
			ID:       st.GetHeader().GetMemberId(),
			Leader:   st.GetLeaderId(),
			Zone:     st.GetZone(),
			State:    st.GetRaftState(),
			Term:     st.GetHeader().GetRaftTerm(),
			Commit:   st.GetCommitIndex(),
			Applied:  st.GetAppliedIndex(),
		})
	}

	header := []string{"ENDPOINT", "ID", "LEADER", "ZONE", "STATE", "TERM", "COMMIT", "APPLIED", "ERROR"}

	err := printTable(cmd, rows, header, func(r statusRow) []string {
		u := func(v uint64) string { return strconv.FormatUint(v, 10) }

		return []string{
			r.Endpoint, u(r.ID), u(r.Leader), r.Zone, r.State, u(r.Term), u(r.Commit), u(r.Applied), r.Error,
		}
	})
	if err != nil {
		return err
	}

	if warning := placementWarning(rows); warning != "" && !cmd.json {
		fmt.Fprintln(cmd.out, "warning:", warning)
	}

	return nil
}

// placementWarning reports a zone that holds a majority of the nodes listed:
// losing that zone loses the quorum. It only judges nodes that answered and
// carry a zone label, and the endpoints should list every voter.
func placementWarning(rows []statusRow) string {
	perZone := map[string]int{}
	labelled := 0

	for _, r := range rows {
		if r.Error == "" && r.Zone != "" {
			perZone[r.Zone]++
			labelled++
		}
	}

	if labelled < 2 {
		return ""
	}

	for _, zone := range slices.Sorted(maps.Keys(perZone)) {
		if n := perZone[zone]; n > labelled/2 {
			return fmt.Sprintf("zone %q holds %d of %d nodes: losing it loses the quorum", zone, n, labelled)
		}
	}

	return ""
}

func parseID(s string) (uint64, error) {
	id, err := strconv.ParseUint(s, 10, 64)
	if err != nil || id == 0 {
		return 0, fmt.Errorf("invalid node ID %q", s)
	}

	return id, nil
}

// printRows writes rows as JSON, or with write as a table.
func (cmd *command) printRows(rows any, write func(w *tabwriter.Writer)) error {
	if cmd.json {
		enc := json.NewEncoder(cmd.out)
		enc.SetIndent("", "  ")

		if err := enc.Encode(rows); err != nil {
			return fmt.Errorf("writing output: %w", err)
		}

		return nil
	}

	w := tabwriter.NewWriter(cmd.out, 0, 4, 2, ' ', 0)
	write(w)

	if err := w.Flush(); err != nil {
		return fmt.Errorf("writing output: %w", err)
	}

	return nil
}

func (cmd *command) printResult(result map[string]any) error {
	return cmd.printRows(result, func(w *tabwriter.Writer) {
		for _, k := range slices.Sorted(maps.Keys(result)) {
			fmt.Fprintf(w, "%s\t%v\n", k, result[k])
		}
	})
}
