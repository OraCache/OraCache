package benchcatalog

import (
	"bytes"
	"math"
	"testing"
)

func config(dist string) Config {
	return Config{Keys: 2048, Seed: 11, Distribution: dist, GetPercent: 90, MissRate: 5, HotTraffic: 80, HotKeys: 20, ZipfS: 1.1, Sizes: "256:60,1024:30,10240:9,65536:1"}
}

func TestDistributions(t *testing.T) {
	for _, dist := range []string{"uniform", "hotspot", "zipf"} {
		c := config(dist)
		p, err := New(c)
		if err != nil {
			t.Fatal(err)
		}
		repeat, _ := New(c)
		c.Seed++
		other, _ := New(c)
		const n = 2048 * 100
		freq := make([]int, p.Keys)
		var gets, missing, hot, changed int
		for i := range n {
			r := p.At(uint64(i))
			if r != repeat.At(uint64(i)) {
				t.Fatal("unstable plan")
			}
			if r != other.At(uint64(i)) {
				changed++
			}
			freq[r.Key]++
			if r.Write && r.Missing {
				t.Fatal("write to missing namespace")
			}
			if !r.Write {
				gets++
				if p.Hot(r.Key) {
					hot++
				}
				if r.Missing {
					missing++
				}
			}
		}
		if changed == 0 || math.Abs(float64(gets)/n-.9) > .005 || math.Abs(float64(missing)/float64(gets)-.05) > .005 {
			t.Fatal(dist, gets, missing)
		}
		switch dist {
		case "uniform":
			for _, f := range freq {
				if f != 100 {
					t.Fatal(f)
				}
			}
		case "hotspot":
			if math.Abs(float64(hot)/float64(gets)-.8) > .01 {
				t.Fatal(hot)
			}
		case "zipf":
			if freq[0] < 2*freq[1] || freq[1] < 15*freq[100] {
				t.Fatal(freq[:3])
			}
		}
	}
}

func TestSizesAndProduct(t *testing.T) {
	p, err := New(config("uniform"))
	if err != nil {
		t.Fatal(err)
	}
	counts := map[int]int{}
	for i := range p.Keys {
		n := p.Size(i)
		counts[n]++
		seed := Payload(i, n, 0)
		data := Payload(i, n, 567)
		token, ok := CheckPayload(i, seed, data)
		if token != 567 || !ok || len(data) != n || bytes.Equal(seed, data) {
			t.Fatal(i)
		}
		data[n-1] ^= 1
		if _, ok := CheckPayload(i, seed, data); ok {
			t.Fatal("corruption accepted")
		}
	}
	if counts[256] != 1229 || counts[1024] != 614 || counts[10240] != 185 || counts[65536] != 20 {
		t.Fatal(counts)
	}
}
func TestParameters(t *testing.T) {
	for _, spec := range []string{"65536:100", "256:100"} {
		c := config("zipf")
		c.Keys = 1
		c.Sizes = spec
		p, err := New(c)
		if err != nil || p.At(7).Key != 0 {
			t.Fatal(err)
		}
	}
	for _, spec := range []string{"", "256:0", "256:101", "2048:100", "1024:50"} {
		c := config("uniform")
		c.Sizes = spec
		if _, err := New(c); err == nil {
			t.Fatal(spec)
		}
	}
	c := config("hotspot")
	c.HotTraffic = 95
	c.HotKeys = 5
	p, _ := New(c)
	hot := 0
	for i := range 10000 {
		if p.Hot(p.At(uint64(i)).Key) {
			hot++
		}
	}
	if hot < 9400 || hot > 9600 {
		t.Fatal(hot)
	}
}
