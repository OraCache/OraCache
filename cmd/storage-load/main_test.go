package main

import (
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCatchUpPolicy(t *testing.T) {
	if !dropLate(12*time.Millisecond, false) || dropLate(12*time.Millisecond, true) || dropLate(5*time.Millisecond, false) {
		t.Fatal("only the default policy drops arrivals more than 5 ms late")
	}
}

func TestClassifyPendingLimit(t *testing.T) {
	if got := classify(status.Error(codes.ResourceExhausted, "too many pending requests")); got != "ResourceExhausted: too many pending requests" {
		t.Fatal(got)
	}
	if got := classify(status.Error(codes.ResourceExhausted, "unrecognized message")); got != "ResourceExhausted" {
		t.Fatal(got)
	}
}

func TestProfileAndRouting(t *testing.T) {
	var reads [3]int
	for i := 0; i < 10500; i++ {
		a, b := request(i, 1, 3, defaultKeys, false), request(i, 1, 3, defaultKeys, true)
		if a.write != b.write || a.key != b.key || a.node != 1 {
			t.Fatalf("workload changed at %d: %v %v", i, a, b)
		}
		if b.write {
			if b.node != 1 {
				t.Fatal(b)
			}
		} else {
			reads[b.node]++
		}
	}
	if reads != [3]int{3334, 3333, 3333} {
		t.Fatal(reads)
	}
}

func TestRequestCoversConfiguredKeys(t *testing.T) {
	for _, keys := range []int{1, 21, 149, defaultKeys} {
		gets, puts := make(map[int]bool), make(map[int]bool)
		for i := 0; i < 21*keys; i++ {
			o := request(i, 0, 1, keys, false)
			if o.key < 0 || o.key >= keys {
				t.Fatalf("key %d outside %d-key set", o.key, keys)
			}
			if o.write {
				puts[o.key] = true
			} else {
				gets[o.key] = true
			}
		}
		if len(gets) != keys || len(puts) != keys {
			t.Fatalf("%d keys: GET used %d, PUT used %d", keys, len(gets), len(puts))
		}
	}
}

func TestScheduleCoversEverySlot(t *testing.T) {
	for _, rate := range []int{21, 1050, 10500} {
		prev := 0
		for ms := 1; ms <= 1000; ms++ {
			n := due(rate, time.Duration(ms)*time.Millisecond)
			if n < prev || n-prev > 11 {
				t.Fatalf("bad schedule at %d", ms)
			}
			prev = n
		}
		if prev != rate {
			t.Fatalf("scheduled %d, want %d", prev, rate)
		}
	}
}
