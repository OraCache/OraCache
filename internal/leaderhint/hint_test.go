package leaderhint

import "testing"

func TestMergeOrderIndependentAndConflictSticky(t *testing.T) {
	observations := []Observation{{NodeID: "old", Term: 3}, {NodeID: "a", Term: 4}, {NodeID: "b", Term: 4}}
	for i := range observations {
		for j := range observations {
			if i == j {
				continue
			}
			k := 3 - i - j
			var got Observation
			for _, n := range []Observation{observations[i], observations[j], observations[k]} {
				got = Merge(got, n)
			}
			if got.Term != 4 || got.NodeID != "" {
				t.Fatalf("order %d %d %d: %v", i, j, k, got)
			}
			if next := Merge(got, Observation{NodeID: "a", Term: 4}); next != got {
				t.Fatalf("conflict reset: %v", next)
			}
			if next := Merge(got, Observation{NodeID: "a", Term: 5}); next.Term != 5 || next.NodeID != "a" {
				t.Fatalf("new term not applied: %v", next)
			}
		}
	}
}
