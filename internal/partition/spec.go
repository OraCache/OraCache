package partition

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// ParseSpec разбирает спецификацию партиций во множество идентификаторов.
// Поддерживаемый синтаксис:
//
//	all              все партиции [0,count)
//	even / odd       чётные или нечётные партиции
//	a-b             диапазон включительно
//	0,2,5-9,odd      список элементов через запятую
//
// Спецификация используется для статической топологии внутри шарда в PoC;
// размещение партиций задаёт Control Plane, а не этот парсер. Все значения
// вне [0,count) отклоняются.
func ParseSpec(spec string, count uint32) ([]uint32, error) {
	if count == 0 {
		return nil, fmt.Errorf("oracache/partition: count must be positive")
	}
	seen := make(map[uint32]struct{}, count)
	add := func(id uint32) error {
		if id >= count {
			return fmt.Errorf("oracache/partition: partition %d is outside [0,%d)", id, count)
		}
		seen[id] = struct{}{}
		return nil
	}
	for _, item := range strings.Split(spec, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			return nil, fmt.Errorf("oracache/partition: empty item in spec %q", spec)
		}
		switch item {
		case "all":
			for p := uint32(0); p < count; p++ {
				seen[p] = struct{}{}
			}
			continue
		case "even":
			for p := uint32(0); p < count; p += 2 {
				seen[p] = struct{}{}
			}
			continue
		case "odd":
			for p := uint32(1); p < count; p += 2 {
				seen[p] = struct{}{}
			}
			continue
		}
		start, end, err := parseRange(item, count)
		if err != nil {
			return nil, err
		}
		for p := start; p <= end; p++ {
			if err := add(p); err != nil {
				return nil, err
			}
		}
	}
	if len(seen) == 0 {
		return nil, fmt.Errorf("oracache/partition: spec %q selects no partitions", spec)
	}
	out := make([]uint32, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	slices.Sort(out)
	return out, nil
}

func parseRange(item string, count uint32) (uint32, uint32, error) {
	parts := strings.Split(item, "-")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	switch len(parts) {
	case 1:
		value, err := parseID(parts[0])
		if err != nil {
			return 0, 0, err
		}
		return value, value, nil
	case 2:
		start, err := parseID(parts[0])
		if err != nil {
			return 0, 0, err
		}
		end, err := parseID(parts[1])
		if err != nil {
			return 0, 0, err
		}
		if start > end {
			return 0, 0, fmt.Errorf("oracache/partition: range %q starts after it ends", item)
		}
		return start, end, nil
	default:
		return 0, 0, fmt.Errorf("oracache/partition: malformed range %q", item)
	}
}

func parseID(text string) (uint32, error) {
	value, err := strconv.ParseUint(text, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("oracache/partition: invalid partition id %q", text)
	}
	return uint32(value), nil
}

// ContainsSpec сообщает, выбирает ли spec партицию id. Удобно для CLI и
// проверок; для больших наборов используйте ParseSpec.
func ContainsSpec(spec string, count, id uint32) (bool, error) {
	ids, err := ParseSpec(spec, count)
	if err != nil {
		return false, err
	}
	return slices.Contains(ids, id), nil
}
