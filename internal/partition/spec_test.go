package partition_test

import (
	"slices"
	"testing"

	"github.com/OraCache/oracache/internal/partition"
)

func TestParseSpecForms(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		spec  string
		count uint32
		want  []uint32
	}{
		{name: "all", spec: "all", count: 4, want: []uint32{0, 1, 2, 3}},
		{name: "even", spec: "even", count: 4, want: []uint32{0, 2}},
		{name: "odd", spec: "odd", count: 4, want: []uint32{1, 3}},
		{name: "range", spec: "1-3", count: 4, want: []uint32{1, 2, 3}},
		{name: "single", spec: "2", count: 4, want: []uint32{2}},
		{name: "list", spec: "0,3", count: 4, want: []uint32{0, 3}},
		{name: "mixed", spec: "0,2,1", count: 4, want: []uint32{0, 1, 2}},
		{name: "dedup", spec: "1,1,even", count: 4, want: []uint32{0, 1, 2}},
		{name: "whitespace", spec: " 1 - 2 , odd ", count: 4, want: []uint32{1, 2, 3}},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := partition.ParseSpec(tt.spec, tt.count)
			if err != nil {
				t.Fatalf("ParseSpec: %v", err)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("ParseSpec(%q,%d) = %v, want %v", tt.spec, tt.count, got, tt.want)
			}
		})
	}
}

func TestParseSpecErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		spec  string
		count uint32
	}{
		{name: "zero_count", spec: "all", count: 0},
		{name: "empty", spec: "", count: 4},
		{name: "empty_item", spec: "0,", count: 4},
		{name: "out_of_range", spec: "4", count: 4},
		{name: "range_out_of_range", spec: "2-9", count: 4},
		{name: "reversed_range", spec: "3-1", count: 4},
		{name: "not_a_number", spec: "abc", count: 4},
		{name: "negative", spec: "-1", count: 4},
		{name: "malformed_range", spec: "1-2-3", count: 4},
		{name: "keyword_with_range", spec: "even-3", count: 4},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := partition.ParseSpec(tt.spec, tt.count); err == nil {
				t.Fatalf("ParseSpec(%q,%d) succeeded, want error", tt.spec, tt.count)
			}
		})
	}
}

func TestParseSpecIsDeterministic(t *testing.T) {
	t.Parallel()
	first, err := partition.ParseSpec("odd,1-2", 8)
	if err != nil {
		t.Fatalf("ParseSpec: %v", err)
	}
	for i := 0; i < 20; i++ {
		got, err := partition.ParseSpec("odd,1-2", 8)
		if err != nil {
			t.Fatalf("ParseSpec: %v", err)
		}
		if !slices.Equal(got, first) {
			t.Fatalf("iteration %d = %v, want %v", i, got, first)
		}
	}
	want := []uint32{1, 2, 3, 5, 7}
	if !slices.Equal(first, want) {
		t.Fatalf("parsed = %v, want %v", first, want)
	}
}

func TestContainsSpec(t *testing.T) {
	t.Parallel()
	if ok, err := partition.ContainsSpec("even", 4, 2); err != nil || !ok {
		t.Fatalf("ContainsSpec(even,2) = %v, %v", ok, err)
	}
	if ok, err := partition.ContainsSpec("even", 4, 1); err != nil || ok {
		t.Fatalf("ContainsSpec(even,1) = %v, %v", ok, err)
	}
	if _, err := partition.ContainsSpec("bogus", 4, 1); err == nil {
		t.Fatal("ContainsSpec accepted a bad spec")
	}
}
