package statemachine_test

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/statemachine"
)

func openDB(t *testing.T) *pebble.DB {
	t.Helper()

	db, err := pebble.Open("db", &pebble.Options{FS: vfs.NewMem()})
	if err != nil {
		t.Fatalf("opening pebble: %v", err)
	}

	t.Cleanup(func() { _ = db.Close() })

	return db
}

type harness struct {
	t     *testing.T
	db    *pebble.DB
	sm    *statemachine.StateMachine
	index uint64
}

func newHarness(t *testing.T, opts statemachine.Options) *harness {
	t.Helper()

	db := openDB(t)

	sm, err := statemachine.Open(db, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	return &harness{t: t, db: db, sm: sm}
}

// apply applies one proposal as the next entry and commits it.
func (h *harness) apply(p *kvpb.Proposal) *kvpb.Result {
	h.t.Helper()

	h.index++

	b := h.sm.NewBatch()
	defer b.Close()

	res, err := b.Apply(h.index, 1, p)
	if err != nil {
		h.t.Fatalf("Apply: %v", err)
	}

	if err := b.Commit(false); err != nil {
		h.t.Fatalf("Commit: %v", err)
	}

	return res
}

func (h *harness) put(key, value string) *kvpb.PutResponse {
	h.t.Helper()

	return h.apply(putProposal(key, value, nil)).GetPut()
}

func (h *harness) txn(req *kvpb.TxnRequest) *kvpb.TxnResponse {
	h.t.Helper()

	res := h.apply(&kvpb.Proposal{RequestId: req.GetRequestId(), Operation: &kvpb.Proposal_Txn{Txn: req}})
	if res.GetError() != "" {
		h.t.Fatalf("txn failed validation: %s", res.GetError())
	}

	return res.GetTxn()
}

func (h *harness) get(key string) *kvpb.KeyValue {
	h.t.Helper()

	kvs := h.rangeKeys(&kvpb.RangeRequest{Key: []byte(key)}).GetKvs()
	if len(kvs) == 0 {
		return nil
	}

	return kvs[0]
}

func (h *harness) rangeKeys(req *kvpb.RangeRequest) *kvpb.RangeResponse {
	h.t.Helper()

	v := h.sm.NewView()
	defer v.Close()

	resp, err := v.Range(req)
	if err != nil {
		h.t.Fatalf("Range: %v", err)
	}

	return resp
}

func putProposal(key, value string, id []byte) *kvpb.Proposal {
	return &kvpb.Proposal{
		RequestId: id,
		Operation: &kvpb.Proposal_Put{Put: &kvpb.PutRequest{Key: []byte(key), Value: []byte(value)}},
	}
}

func putOp(key, value string) *kvpb.RequestOp {
	return &kvpb.RequestOp{Request: &kvpb.RequestOp_RequestPut{
		RequestPut: &kvpb.PutRequest{Key: []byte(key), Value: []byte(value)},
	}}
}

func rangeOp(key string) *kvpb.RequestOp {
	return &kvpb.RequestOp{Request: &kvpb.RequestOp_RequestRange{
		RequestRange: &kvpb.RangeRequest{Key: []byte(key)},
	}}
}

func modRevision(key string, result kvpb.Compare_Result, rev int64) *kvpb.Compare {
	return &kvpb.Compare{Key: []byte(key), Result: result, Target: &kvpb.Compare_ModRevision{ModRevision: rev}}
}

func keys(kvs []*kvpb.KeyValue) []string {
	out := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, string(kv.GetKey()))
	}

	return out
}

func TestPutRevisions(t *testing.T) {
	t.Parallel()

	h := newHarness(t, statemachine.Options{})

	if rev := h.put("a", "1").GetHeader().GetRevision(); rev != 1 {
		t.Fatalf("first put revision = %d, want 1", rev)
	}

	h.put("b", "x")
	h.put("a", "2")

	kv := h.get("a")
	if string(kv.GetValue()) != "2" || kv.GetCreateRevision() != 1 || kv.GetModRevision() != 3 || kv.GetVersion() != 2 {
		t.Fatalf("a = %+v, want value 2, create 1, mod 3, version 2", kv)
	}

	if got := h.sm.Applied(); got.GetRevision() != 3 || got.GetIndex() != 3 {
		t.Fatalf("applied = %+v, want revision 3 at index 3", got)
	}
}

func TestRange(t *testing.T) {
	t.Parallel()

	h := newHarness(t, statemachine.Options{})
	for _, k := range []string{"a/1", "a/2", "a/3", "b/1"} {
		h.put(k, "v")
	}

	tests := []struct {
		name     string
		req      *kvpb.RangeRequest
		want     []string
		wantMore bool
	}{
		{name: "single key", req: &kvpb.RangeRequest{Key: []byte("a/2")}, want: []string{"a/2"}},
		{name: "missing key", req: &kvpb.RangeRequest{Key: []byte("a/9")}, want: []string{}},
		{
			name: "prefix",
			req:  &kvpb.RangeRequest{Key: []byte("a/"), RangeEnd: []byte("a0")},
			want: []string{"a/1", "a/2", "a/3"},
		},
		{
			name:     "limit",
			req:      &kvpb.RangeRequest{Key: []byte("a/"), RangeEnd: []byte("a0"), Limit: 2},
			want:     []string{"a/1", "a/2"},
			wantMore: true,
		},
		{
			name: "from key on",
			req:  &kvpb.RangeRequest{Key: []byte("a/3"), RangeEnd: []byte{0}},
			want: []string{"a/3", "b/1"},
		},
		{name: "everything", req: &kvpb.RangeRequest{RangeEnd: []byte{0}}, want: []string{"a/1", "a/2", "a/3", "b/1"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp := h.rangeKeys(tt.req)
			if got := keys(resp.GetKvs()); fmt.Sprint(got) != fmt.Sprint(tt.want) || resp.GetMore() != tt.wantMore {
				t.Fatalf("Range = %v more=%v, want %v more=%v", got, resp.GetMore(), tt.want, tt.wantMore)
			}

			if resp.GetHeader().GetRevision() != 4 {
				t.Fatalf("header revision = %d, want 4", resp.GetHeader().GetRevision())
			}
		})
	}
}

func TestDeleteRange(t *testing.T) {
	t.Parallel()

	h := newHarness(t, statemachine.Options{})
	for _, k := range []string{"a/1", "a/2", "b/1"} {
		h.put(k, "v")
	}

	del := func(key, end string) *kvpb.DeleteRangeResponse {
		return h.apply(&kvpb.Proposal{Operation: &kvpb.Proposal_DeleteRange{
			DeleteRange: &kvpb.DeleteRangeRequest{Key: []byte(key), RangeEnd: []byte(end)},
		}}).GetDeleteRange()
	}

	if resp := del("a/", "a0"); resp.GetDeleted() != 2 || resp.GetHeader().GetRevision() != 4 {
		t.Fatalf("delete a/* = %+v, want 2 deleted at revision 4", resp)
	}

	// Deleting nothing writes nothing, so it takes no revision.
	if resp := del("zzz", ""); resp.GetDeleted() != 0 || resp.GetHeader().GetRevision() != 4 {
		t.Fatalf("delete of a missing key = %+v, want 0 deleted at revision 4", resp)
	}

	if got := keys(h.rangeKeys(&kvpb.RangeRequest{RangeEnd: []byte{0}}).GetKvs()); fmt.Sprint(got) != "[b/1]" {
		t.Fatalf("remaining keys = %v, want [b/1]", got)
	}
}

func TestTxnCompares(t *testing.T) {
	t.Parallel()

	h := newHarness(t, statemachine.Options{})
	h.put("k", "v")  // revision 1
	h.put("p/1", "") // revision 2
	h.put("p/2", "") // revision 3

	eq, ne, lt, gt := kvpb.Compare_RESULT_EQUAL, kvpb.Compare_RESULT_NOT_EQUAL,
		kvpb.Compare_RESULT_LESS, kvpb.Compare_RESULT_GREATER

	value := func(r kvpb.Compare_Result, key, v string) *kvpb.Compare {
		return &kvpb.Compare{Key: []byte(key), Result: r, Target: &kvpb.Compare_Value{Value: []byte(v)}}
	}
	version := func(r kvpb.Compare_Result, key string, v int64) *kvpb.Compare {
		return &kvpb.Compare{Key: []byte(key), Result: r, Target: &kvpb.Compare_Version{Version: v}}
	}
	count := func(r kvpb.Compare_Result, n int64) *kvpb.Compare {
		return &kvpb.Compare{
			Key:      []byte("p/"),
			RangeEnd: []byte("p0"),
			Result:   r,
			Target:   &kvpb.Compare_RangeCount{RangeCount: n},
		}
	}

	tests := []struct {
		name string
		cmp  *kvpb.Compare
		want bool
	}{
		{name: "mod revision equal", cmp: modRevision("k", eq, 1), want: true},
		{name: "mod revision stale", cmp: modRevision("k", eq, 0), want: false},
		{name: "absent key has mod revision 0", cmp: modRevision("missing", eq, 0), want: true},
		{name: "mod revision greater", cmp: modRevision("k", gt, 0), want: true},
		{name: "version less", cmp: version(lt, "k", 2), want: true},
		{name: "value equal", cmp: value(eq, "k", "v"), want: true},
		{name: "value not equal", cmp: value(ne, "k", "v"), want: false},
		{name: "absent key never matches a value", cmp: value(ne, "missing", "v"), want: false},
		{name: "range count equal", cmp: count(eq, 2), want: true},
		{name: "range empty", cmp: count(eq, 0), want: false},
		{name: "range count greater", cmp: count(gt, 1), want: true},
		{name: "range count less", cmp: count(lt, 2), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			v := h.sm.NewView()
			defer v.Close()

			resp, err := v.Txn(&kvpb.TxnRequest{Compare: []*kvpb.Compare{tt.cmp}})
			if err != nil {
				t.Fatalf("Txn: %v", err)
			}

			if resp.GetSucceeded() != tt.want {
				t.Fatalf("succeeded = %v, want %v", resp.GetSucceeded(), tt.want)
			}
		})
	}
}

func TestTxnBranchesAndRevisions(t *testing.T) {
	t.Parallel()

	h := newHarness(t, statemachine.Options{})
	h.put("a", "1") // revision 1

	eq := kvpb.Compare_RESULT_EQUAL

	// Success: both writes share revision 2, and the read in the branch sees
	// the earlier write.
	resp := h.txn(&kvpb.TxnRequest{
		Compare: []*kvpb.Compare{modRevision("a", eq, 1)},
		Success: []*kvpb.RequestOp{putOp("a", "2"), putOp("b", "1"), rangeOp("b")},
	})
	if !resp.GetSucceeded() || resp.GetHeader().GetRevision() != 2 {
		t.Fatalf("txn = %+v, want success at revision 2", resp)
	}

	if got := resp.GetResponses()[2].GetResponseRange().GetKvs(); len(got) != 1 || got[0].GetModRevision() != 2 {
		t.Fatalf("read in branch = %v, want b at revision 2", got)
	}

	if h.get("a").GetModRevision() != 2 || h.get("b").GetModRevision() != 2 {
		t.Fatal("writes of one txn must share its revision")
	}

	// Failure: the failure branch only reads, so no revision is taken.
	resp = h.txn(&kvpb.TxnRequest{
		Compare: []*kvpb.Compare{modRevision("a", eq, 1)},
		Success: []*kvpb.RequestOp{putOp("a", "3")},
		Failure: []*kvpb.RequestOp{rangeOp("a")},
	})
	if resp.GetSucceeded() || resp.GetHeader().GetRevision() != 2 {
		t.Fatalf("txn = %+v, want failure at revision 2", resp)
	}

	if cur := resp.GetResponses()[0].GetResponseRange().GetKvs()[0]; string(cur.GetValue()) != "2" {
		t.Fatalf("failure branch read %q, want the current value 2", cur.GetValue())
	}
}

func TestReadOnlyViewRejectsWrites(t *testing.T) {
	t.Parallel()

	h := newHarness(t, statemachine.Options{})

	v := h.sm.NewView()
	defer v.Close()

	if _, err := v.Txn(&kvpb.TxnRequest{Success: []*kvpb.RequestOp{putOp("a", "1")}}); err == nil {
		t.Fatal("a view served a writing transaction")
	}
}

func TestInvalidProposalIsRecorded(t *testing.T) {
	t.Parallel()

	h := newHarness(t, statemachine.Options{})

	res := h.apply(putProposal("", "v", nil))
	if res.GetError() == "" {
		t.Fatal("a put with an empty key was applied")
	}

	if got := h.sm.Applied(); got.GetIndex() != 1 || got.GetRevision() != 0 {
		t.Fatalf("applied = %+v, want index 1 and no revision", got)
	}
}

func TestDedup(t *testing.T) {
	t.Parallel()

	h := newHarness(t, statemachine.Options{DedupWindow: 3})
	id := []byte("request-1")

	first := h.apply(putProposal("a", "1", id))
	second := h.apply(putProposal("a", "1", id))

	if !proto.Equal(first, second) {
		t.Fatalf("retried request got %v, want the first result %v", second, first)
	}

	if kv := h.get("a"); kv.GetVersion() != 1 {
		t.Fatalf("a has version %d, want 1: the retry was executed", kv.GetVersion())
	}

	// After more than the window of revisions, the record is forgotten.
	for i := range 4 {
		h.put(fmt.Sprintf("other/%d", i), "v")
	}

	h.apply(putProposal("a", "1", id))

	if kv := h.get("a"); kv.GetVersion() != 2 {
		t.Fatalf("a has version %d, want 2 after the dedup window passed", kv.GetVersion())
	}
}

func TestConfChangeMembers(t *testing.T) {
	t.Parallel()

	h := newHarness(t, statemachine.Options{})

	change := func(typ raftpb.ConfChangeType, id uint64, url string) {
		t.Helper()

		h.index++

		b := h.sm.NewBatch()
		defer b.Close()

		cc := &raftpb.ConfChange{Type: typ.Enum(), NodeId: new(id)}

		var member *kvpb.Member
		if url != "" {
			member = &kvpb.Member{Id: id, PeerUrl: url}
		}

		if err := b.ApplyConfChange(h.index, 1, cc, &raftpb.ConfState{Voters: []uint64{id}}, member); err != nil {
			t.Fatalf("ApplyConfChange: %v", err)
		}

		if err := b.Commit(false); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}

	change(raftpb.ConfChangeType_ConfChangeAddNode, 2, "http://n2")
	change(raftpb.ConfChangeType_ConfChangeAddNode, 1, "http://n1")
	change(raftpb.ConfChangeType_ConfChangeAddLearnerNode, 3, "http://n3")
	change(raftpb.ConfChangeType_ConfChangeAddNode, 3, "") // promote keeps the URL
	change(raftpb.ConfChangeType_ConfChangeRemoveNode, 2, "")

	var b strings.Builder
	for _, m := range h.sm.Applied().GetMembers() {
		fmt.Fprintf(&b, "%d=%s,learner=%v;", m.GetId(), m.GetPeerUrl(), m.GetLearner())
	}

	want := "1=http://n1,learner=false;3=http://n3,learner=false;"
	if got := b.String(); got != want {
		t.Fatalf("members = %s, want %s", got, want)
	}
}

func TestStatePersists(t *testing.T) {
	t.Parallel()

	h := newHarness(t, statemachine.Options{})
	h.put("a", "1")

	reopened, err := statemachine.Open(h.db, statemachine.Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if !proto.Equal(reopened.Applied(), h.sm.Applied()) {
		t.Fatalf("reopened applied state = %v, want %v", reopened.Applied(), h.sm.Applied())
	}
}

// randomLog returns a reproducible mix of writes over a small key set.
func randomLog(seed uint64, n int) []*kvpb.Proposal {
	rng := rand.New(rand.NewPCG(seed, seed))
	log := make([]*kvpb.Proposal, 0, n)

	for i := range n {
		key := fmt.Sprintf("k/%d", rng.IntN(8))
		id := fmt.Appendf(nil, "id-%d", rng.IntN(n))

		switch rng.IntN(3) {
		case 0:
			log = append(log, putProposal(key, strconv.Itoa(i), id))
		case 1:
			log = append(log, &kvpb.Proposal{RequestId: id, Operation: &kvpb.Proposal_DeleteRange{
				DeleteRange: &kvpb.DeleteRangeRequest{Key: []byte(key)},
			}})
		default:
			log = append(log, &kvpb.Proposal{RequestId: id, Operation: &kvpb.Proposal_Txn{Txn: &kvpb.TxnRequest{
				Compare: []*kvpb.Compare{modRevision(key, kvpb.Compare_RESULT_GREATER, int64(rng.IntN(i+1)))},
				Success: []*kvpb.RequestOp{putOp(key, "s"), putOp("k/shared", "s")},
				Failure: []*kvpb.RequestOp{rangeOp(key)},
			}}})
		}
	}

	return log
}

func export(t *testing.T, sm *statemachine.StateMachine) []byte {
	t.Helper()

	var buf bytes.Buffer
	if _, err := sm.ExportSnapshot(&buf); err != nil {
		t.Fatalf("ExportSnapshot: %v", err)
	}

	return buf.Bytes()
}

func TestDeterministic(t *testing.T) {
	t.Parallel()

	log := randomLog(42, 500)
	states := make([][]byte, 0, 2)
	results := make([][]*kvpb.Result, 0, 2)

	for range 2 {
		h := newHarness(t, statemachine.Options{DedupWindow: 50})
		res := make([]*kvpb.Result, 0, len(log))

		for _, p := range log {
			res = append(res, h.apply(p))
		}

		states = append(states, export(t, h.sm))
		results = append(results, res)
	}

	if !bytes.Equal(states[0], states[1]) {
		t.Fatal("applying the same log twice produced different states")
	}

	for i := range log {
		if !proto.Equal(results[0][i], results[1][i]) {
			t.Fatalf("entry %d: results differ: %v vs %v", i, results[0][i], results[1][i])
		}
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	t.Parallel()

	src := newHarness(t, statemachine.Options{})
	for _, p := range randomLog(7, 200) {
		src.apply(p)
	}

	var buf bytes.Buffer

	exported, err := src.sm.ExportSnapshot(&buf)
	if err != nil {
		t.Fatalf("ExportSnapshot: %v", err)
	}

	dst := newHarness(t, statemachine.Options{})
	dst.put("stale", "must be replaced")

	b := dst.db.NewBatch()

	imported, err := dst.sm.ImportSnapshot(b, bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("ImportSnapshot: %v", err)
	}

	if err := b.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}

	dst.sm.SetApplied(imported)

	if !proto.Equal(imported, exported) {
		t.Fatalf("imported applied state %v, want %v", imported, exported)
	}

	if !bytes.Equal(export(t, dst.sm), buf.Bytes()) {
		t.Fatal("state after import differs from the snapshot")
	}

	if dst.get("stale") != nil {
		t.Fatal("import kept a key that is not in the snapshot")
	}
}

func TestImportRejectsCorruptSnapshots(t *testing.T) {
	t.Parallel()

	src := newHarness(t, statemachine.Options{})
	src.put("a", "1")
	good := export(t, src.sm)

	tests := map[string][]byte{
		"truncated":        good[:len(good)-1],
		"no applied state": {},
		"key outside":      {3, 'x', '/', 'y', 0},
	}

	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dst := newHarness(t, statemachine.Options{})
			b := dst.db.NewBatch()

			defer b.Close()

			if _, err := dst.sm.ImportSnapshot(b, bytes.NewReader(data)); err == nil {
				t.Fatal("corrupt snapshot imported without error")
			}
		})
	}
}

func TestValidation(t *testing.T) {
	t.Parallel()

	limits := statemachine.Limits{MaxKeyBytes: 4, MaxOps: 2, MaxRangeLimit: 10}
	cmp := modRevision("k", kvpb.Compare_RESULT_EQUAL, 0)

	tests := map[string]*kvpb.Proposal{
		"empty key":    putProposal("", "v", nil),
		"long key":     putProposal("12345", "v", nil),
		"no operation": {},
		"too many ops": {Operation: &kvpb.Proposal_Txn{Txn: &kvpb.TxnRequest{
			Success: []*kvpb.RequestOp{putOp("a", ""), putOp("b", ""), putOp("c", "")},
		}}},
		"compare without result": {Operation: &kvpb.Proposal_Txn{Txn: &kvpb.TxnRequest{
			Compare: []*kvpb.Compare{{Key: []byte("k"), Target: &kvpb.Compare_Version{}}},
		}}},
		"range end on a key compare": {Operation: &kvpb.Proposal_Txn{Txn: &kvpb.TxnRequest{
			Compare: []*kvpb.Compare{{
				Key:      cmp.GetKey(),
				RangeEnd: []byte("z"),
				Result:   cmp.GetResult(),
				Target:   cmp.GetTarget(),
			}},
		}}},
	}

	for name, p := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if err := limits.ValidateProposal(p); err == nil {
				t.Fatal("invalid proposal passed validation")
			}
		})
	}

	if err := limits.ValidateProposal(putProposal("ok", "v", nil)); err != nil {
		t.Fatalf("valid proposal rejected: %v", err)
	}
}
