package utils

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func HandleError(err error) (int, bool) {
	st, ok := status.FromError(err)
	if !ok {
		logrus.Fatalf("error was not a status error")
		return -1, false
	}
	if st.Code() == codes.Aborted {
		leaderId, e := strconv.Atoi(st.Message())
		if e != nil {
			logrus.Fatalf("cannot convert leader id to int")
		}
		return leaderId, true
	}
	return -1, false
}
