package utils

import "strconv"

func EncodeFloat64(value float64) []byte {
	return []byte(strconv.FormatFloat(value, 'f', -1, 64))
}

func DecodeFloat64(data []byte) float64 {
	value, _ := strconv.ParseFloat(string(data), 64)
	return value
}
