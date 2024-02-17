package redis

import "errors"

var (
	ErrorWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)
