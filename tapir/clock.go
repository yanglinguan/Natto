package tapir

import (
	"time"
)

// Reads the clock time. The return value will be adjusted by the given skew value.
func ReadClockNano(skew int64) int64 {
	return time.Now().UnixNano() + skew
}

// NOTE: any emulated distribution of the clock skew can be implemented here.
func GenClockSkew(avg int64) int64 {
	return avg
}
