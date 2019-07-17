package vastflow

import (
	"github.com/jack0liu/logs"
	"runtime"
)

func PrintStack() {
	var buf [4096]byte
	n := runtime.Stack(buf[:], false)
	logs.Error("panic ==> %s", string(buf[:n]))
}
