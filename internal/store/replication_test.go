package store

import "testing"

func TestReplaceVersionsPreservesRevisionsAndRejectsInvalidState(t *testing.T) {
	s, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ReplaceVersions("k", []Version{{Rev: 12, Data: []byte("one")}, {Rev: 40, Data: []byte("two")}}); err != nil {
		t.Fatal(err)
	}
	if err := s.ReplaceVersions("k", []Version{{Rev: 5}, {Rev: 4}}); err == nil {
		t.Fatal("accepted unordered versions")
	}
	if got := s.Peek("k", 12); got.LatestRev != 40 || string(got.Version.Data) != "one" {
		t.Fatal(got)
	}
	versions := s.Versions("k")
	versions[0].Data[0] = 'x'
	if string(s.Peek("k", 12).Version.Data) != "one" {
		t.Fatal("shared bytes")
	}
	if err := s.ReplaceVersions("k", nil); err != nil {
		t.Fatal(err)
	}
	if s.Peek("k", 0).Status != GetMiss {
		t.Fatal("key not deleted")
	}
	next, err := s.Put("k", []byte("new"), nil)
	if err != nil || next.Rev != 41 {
		t.Fatal("imported revision sequence not retained", next, err)
	}
	data, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Restore(data); err != nil {
		t.Fatal(err)
	}
}
