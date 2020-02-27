package utils

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"strconv"
)

func ConvertToString(size int, key int64) string {
	format := "%" + strconv.Itoa(size) + "d"
	return fmt.Sprintf(format, key)
}

func ConvertToInt(key string) int64 {
	var i int64
	_, err := fmt.Sscan(key, &i)
	if err != nil {
		logrus.Fatalf("key %v invalid ", key)
	}
	return i
}
