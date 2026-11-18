package fileio_test

import (
	"io"
	"testing"

	"echpochmak/internal/fileio"
)

func TestObjects(t *testing.T) {
	t.Parallel()

	f := fileio.NewIceberg(t.Context(), nil)
	base := "mem://objects-test/backups"

	w, err := f.Create(t.Context(), base+"/c1/a.snap")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if _, err := io.WriteString(w, "hello"); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	if err := f.Write(t.Context(), base+"/c1/a.snap.json", []byte("{}")); err != nil {
		t.Fatal(err)
	}

	if err := f.Write(t.Context(), base+"-other/x", []byte("x")); err != nil {
		t.Fatal(err)
	}

	got, err := f.List(t.Context(), base+"/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	first := len(got) == 2 && got[0].Location == base+"/c1/a.snap" && got[0].Size == 5
	if !first || got[1].Location != base+"/c1/a.snap.json" {
		t.Fatalf("List = %+v, want the two files under the prefix only", got)
	}

	r, err := f.Open(t.Context(), base+"/c1/a.snap")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if data, _ := io.ReadAll(r); string(data) != "hello" {
		t.Fatalf("read %q", data)
	}

	empty, err := f.List(t.Context(), "mem://objects-test/nothing/")
	if err != nil || len(empty) != 0 {
		t.Fatalf("List of an empty prefix = %v, %v", empty, err)
	}
}
