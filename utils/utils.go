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

func ConvertFloatToString(val float64) string {
	format := "%.2f"
	return fmt.Sprintf(format, val)
}

func ConvertToFloat(key string) float64 {
	var f float64
	_, err := fmt.Sscan(key, &f)
	if err != nil {
		logrus.Fatalf("key %v invalid", key)
	}
	return f
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

//Calculates the percentile in a histogram
func CalHistPctl(hist []int, p float64) int {
	if len(hist) == 0 {
		return -1
	}

	sum := 0
	for _, v := range hist {
		sum += v
	}

	if sum == 0 {
		return 0
	}

	m := float64(sum)
	s := 0
	for i, v := range hist {
		s += v
		if float64(s)/m >= p {
			return i
		}
	}

	return len(hist) - 1
}

func QuickSort64n(arr []int64, s, e int) {
	m := s + 1 // the first element >= arr[s]
	if s < e {
		for i := s + 1; i <= e; i++ {
			if arr[s] > arr[i] {
				arr[m], arr[i] = arr[i], arr[m]
				m++
			}
		}
		arr[m-1], arr[s] = arr[s], arr[m-1]

		QuickSort64n(arr, s, m-2)
		QuickSort64n(arr, m, e)
	}
}
