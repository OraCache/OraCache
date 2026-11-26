// Package benchcatalog describes the synthetic product workload of the benchmark client.
package benchcatalog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
)

type Config struct {
	Keys                                             int
	Seed                                             int64
	Distribution                                     string
	GetPercent, MissRate, HotTraffic, HotKeys, ZipfS float64
	Sizes                                            string
}

type Plan struct {
	Config
	order []int
	cdf   []float64
	sizes []int
	hot   int
}

type Request struct {
	Key            int
	Write, Missing bool
}

func New(c Config) (*Plan, error) {
	if c.Keys < 1 {
		return nil, fmt.Errorf("catalog needs at least one key")
	}
	for _, v := range []float64{c.GetPercent, c.MissRate, c.HotTraffic, c.HotKeys} {
		if math.IsNaN(v) || v < 0 || v > 100 {
			return nil, fmt.Errorf("catalog percentages must be in [0,100]")
		}
	}
	if c.HotKeys <= 0 || c.HotKeys >= 100 || math.IsNaN(c.ZipfS) || math.IsInf(c.ZipfS, 0) || c.ZipfS <= 0 {
		return nil, fmt.Errorf("hot-keys must be in (0,100), zipf-s must be finite and positive")
	}
	rng := rand.New(rand.NewSource(c.Seed))
	p := &Plan{Config: c, order: rng.Perm(c.Keys), sizes: make([]int, c.Keys)}
	p.hot = max(1, min(c.Keys-1, int(math.Round(float64(c.Keys)*c.HotKeys/100))))
	switch c.Distribution {
	case "uniform", "hotspot":
	case "zipf":
		p.cdf = make([]float64, c.Keys)
		var sum float64
		for i := range p.cdf {
			sum += math.Pow(float64(i+1), -c.ZipfS)
			p.cdf[i] = sum
		}
	default:
		return nil, fmt.Errorf("unknown distribution %q", c.Distribution)
	}
	// A separate permutation assigns sizes independently of popularity rank.
	sizeOrder := rng.Perm(c.Keys)
	total, assigned := 0, 0
	for _, group := range strings.Split(c.Sizes, ",") {
		sizeText, weightText, ok := strings.Cut(group, ":")
		size, e1 := strconv.Atoi(sizeText)
		weight, e2 := strconv.Atoi(weightText)
		if !ok || e1 != nil || e2 != nil || (size != 256 && size != 1024 && size != 10240 && size != 65536) || weight < 1 || weight > 100-total {
			return nil, fmt.Errorf("invalid size:percent group %q", group)
		}
		total += weight
		end := int(math.Round(float64(c.Keys) * float64(total) / 100))
		for _, i := range sizeOrder[assigned:end] {
			p.sizes[i] = size
		}
		assigned = end
	}
	if total != 100 {
		return nil, fmt.Errorf("size percentages must sum to 100")
	}
	return p, nil
}

func (p *Plan) Size(i int) int { return p.sizes[i] }
func (p *Plan) Hot(i int) bool { return i < p.hot }

// SplitMix64 finalization, addressed by logical request position and independent lane.
// No shared RNG, precomputed request tape or worker scheduling dependency.
func (p *Plan) draw(pos, lane uint64) float64 {
	x := uint64(p.Seed) + (pos*4+lane+1)*0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return float64((x^(x>>31))>>11) / (1 << 53)
}
func (p *Plan) At(pos uint64) Request {
	r := Request{Write: p.draw(pos, 0)*100 >= p.GetPercent}
	r.Missing = !r.Write && p.draw(pos, 1)*100 < p.MissRate
	u := p.draw(pos, 2)
	switch p.Distribution {
	case "uniform":
		r.Key = p.order[pos%uint64(len(p.order))]
	case "hotspot":
		if p.Keys == 1 || p.draw(pos, 3)*100 < p.HotTraffic {
			r.Key = int(u * float64(p.hot))
		} else {
			r.Key = p.hot + int(u*float64(p.Keys-p.hot))
		}
	case "zipf":
		target := u * p.cdf[len(p.cdf)-1]
		r.Key = sort.Search(len(p.cdf), func(i int) bool { return p.cdf[i] > target })
		r.Key = min(r.Key, p.Keys-1)
	}
	return r
}

// The binary product contains product ID, write token, price in cents and stock,
// followed by immutable product description bytes. Token zero is the seed version.
func Payload(key, size int, token uint64) []byte {
	b := bytes.Repeat([]byte{byte('a' + key%26)}, size)
	binary.LittleEndian.PutUint64(b, uint64(key))
	binary.LittleEndian.PutUint64(b[8:], token)
	binary.LittleEndian.PutUint32(b[16:], uint32(100+key)+uint32(token%1000))
	binary.LittleEndian.PutUint32(b[20:], uint32(token%500))
	return b
}
func CheckPayload(key int, seed, data []byte) (uint64, bool) {
	if len(data) != len(seed) || len(data) < 24 || binary.LittleEndian.Uint64(data) != uint64(key) {
		return 0, false
	}
	token := binary.LittleEndian.Uint64(data[8:])
	return token, binary.LittleEndian.Uint32(data[16:]) == uint32(100+key)+uint32(token%1000) &&
		binary.LittleEndian.Uint32(data[20:]) == uint32(token%500) && bytes.Equal(data[24:], seed[24:])
}
