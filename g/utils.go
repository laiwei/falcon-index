package g

import (
	"encoding/binary"
)

func StringSliceIntersect(a, b []string) []string {
	rt := []string{}

	target := make(map[string]bool)
	for _, f := range a {
		target[f] = true
	}

	for _, f := range b {
		if _, ok := target[f]; ok {
			rt = append(rt, f)
		}
	}

	return rt
}

func BytesToInt64(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf))
}
func Int64ToBytes(i int64) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}
