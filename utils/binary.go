package utils

import (
	"encoding/binary"
	"math"
)

func EncodeFloat64(f float64) string {
	buf := make([]byte, binary.MaxVarintLen64)
	n := math.Float64bits(f)
	binary.PutUvarint(buf, n)
	s := string(buf)
	return s
}

func DecodeFloat64(s string) float64 {
	bf := []byte(s)
	n, _ := binary.Uvarint(bf)
	f := math.Float64frombits(n)
	return f
}
