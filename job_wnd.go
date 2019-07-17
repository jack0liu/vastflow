package vastflow

import (
	"github.com/jack0liu/logs"
	"io/ioutil"
	"math"
	"os"
	"os/exec"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	DefaultWndCapacity = 500
	defaultShrinkLen = 2
	defaultExtendLen = 2
)

var flowWnd = &FlowWnd{
	Capacity: DefaultWndCapacity,
	CurSize:  0,
}

var sys = &SysInfo{}

type SysInfo struct {
	CPU float64
}

type stat struct {
	utime float64
	stime float64
	start float64

	uptime float64
}

type FlowWnd struct {
	sync.RWMutex

	Capacity   int64
	extended   int64
	CurSize    int64

	usedCpuNum int
	markRate   float64

	clk float64

	history stat
}

func InitWnd(capacity int64, cpuNum int, markRate float64) {
	flowWnd.Capacity = capacity
	flowWnd.usedCpuNum = cpuNum
	flowWnd.markRate = markRate

	flowWnd.clk = 100
	if runtime.GOOS == "linux" {
		clkTckStdout, err := exec.Command("getconf", "CLK_TCK").Output()
		if err == nil {
			flowWnd.clk = parseFloat(formatStdOut(clkTckStdout, 0)[0])
		}
	}

}

func checkShrink() {
	t := time.NewTicker(10 * time.Second)
	continueCount := 0
	for {
		select {
		case <-t.C:
			if flowWnd.extended <= 0 {
				break
			}
			flowWnd.Lock()
			if flowWnd.CurSize + defaultShrinkLen < flowWnd.Capacity + flowWnd.extended {
				continueCount++
			} else {
				continueCount = 0
			}
			if continueCount >= 12 {
				flowWnd.Shrink(defaultShrinkLen)
				continueCount = 0
			}
			flowWnd.Unlock()
		}
	}
}


func (fw *FlowWnd) checkShrink() bool {
	if fw.CurSize + defaultShrinkLen <= fw.Capacity {
		return true
	}
	return false
}

func (fw *FlowWnd) Extend(num int64) {
	logs.Info("wnd extended")
	fw.extended = fw.extended + num
}

func (fw *FlowWnd) Shrink(num int64) {
	logs.Info("wnd shrink")
	fw.extended = fw.extended - num
	if fw.extended < 0 {
		fw.extended = 0
	}
}

func (fw *FlowWnd) Inc() {
	fw.CurSize = fw.CurSize + 1
}

func (fw *FlowWnd) Dec() {
	fw.CurSize = fw.CurSize - 1
	if fw.CurSize < 0 {
		fw.CurSize = 0
	}
}

func (fw *FlowWnd) Remains() int64 {
	return fw.Capacity + fw.extended - fw.CurSize
}

func (fw *FlowWnd) IsBelowMark() bool {
	info := fw.calc()
	if info.CPU < fw.markRate*100*float64(fw.usedCpuNum) {
		return true
	}
	return false
}

func (fw *FlowWnd) Print() {
	logs.Info("flow window curSize:%d, total capacity:%d", fw.CurSize, fw.Capacity + fw.extended)
}

func (fw *FlowWnd) calc() *SysInfo {
	if runtime.GOOS != "linux" {
		logs.Debug("not linux, return 0.0")
		return sys
	}

	uptimeFileBytes, err := ioutil.ReadFile(path.Join("/proc", "uptime"))
	if err != nil {
		logs.Error("get uptime fail")
		return sys
	}
	uptime := parseFloat(strings.Split(string(uptimeFileBytes), " ")[0])
	procStatFileBytes, _ := ioutil.ReadFile(path.Join("/proc", strconv.Itoa(os.Getpid()), "stat"))
	splitAfter := strings.SplitAfter(string(procStatFileBytes), ")")
	if len(splitAfter) == 0 || len(splitAfter) == 1 {
		logs.Error("get stat fail")
		return sys
	}
	infos := strings.Split(splitAfter[1], " ")
	st := stat{
		utime:  parseFloat(infos[12]),
		stime:  parseFloat(infos[13]),
		start:  parseFloat(infos[20]) / fw.clk,
		uptime: uptime,
	}

	_stime := 0.0
	_utime := 0.0
	if fw.history.stime != 0 {
		_stime = fw.history.stime
	}

	if fw.history.utime != 0 {
		_utime = fw.history.utime
	}
	total := st.stime - _stime + st.utime - _utime
	total = total / fw.clk

	seconds := st.start - uptime
	if fw.history.uptime != 0 {
		seconds = uptime - fw.history.uptime
	}
	fw.history = st

	seconds = math.Abs(seconds)
	if seconds == 0 {
		seconds = 1
	}
	sys.CPU = (total / seconds) * 100
	logs.Debug("cpu: %f", sys.CPU)
	return sys
}

func parseFloat(val string) float64 {
	floatVal, _ := strconv.ParseFloat(val, 64)
	return floatVal
}

func formatStdOut(stdout []byte, userfulIndex int) []string {
	infoArr := strings.Split(string(stdout), "\n")[userfulIndex]
	ret := strings.Fields(infoArr)
	return ret
}
