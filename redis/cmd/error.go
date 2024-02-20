package main

import "errors"

// 如果 command 的参数数量不对，返回这个错误
func newWrongNumberOfArgsError(cmd string) error {
	return errors.New("ERR wrong number of arguments for '" + cmd + "' command")
}
