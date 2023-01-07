package util

import "errors"

var (
	ERROR_QUIT    string = "QUIT_COMMAND"
	ERROR_EXPIRED error  = errors.New("Key has expired")
)
