package dmap

import (
	"fmt"
	"log"
)

var Logger *log.Logger

func logf(s string, args ...interface{}) {
	if Logger == nil {
		return
	}
	Logger.Output(0, fmt.Sprintf(s, args...))
}
