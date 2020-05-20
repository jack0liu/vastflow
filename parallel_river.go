package vastflow

import (
	"errors"
	"github.com/jack0liu/logs"
	"sync"
)

type ParallelStream interface {
	Append(river Stream) *ParallelRiver
	getRivers() []Stream
}

type ParallelRiver struct {
	attr RiverAttr

	wg     sync.WaitGroup
	rivers []Stream

	id      string
	errStr  string // failed error str
	down    Stream
	state   streamState
	color   string
	waterId string // used for restore
}

func init() {
	RegisterStream(new(ParallelRiver))
}

func (pa *ParallelRiver) setFail(errStr string, headwaters *Headwaters) error {
	pa.state = stateFail
	pa.errStr = errStr
	if err := setFlowEnd(pa.id, stateFail.String(), errStr); err != nil {
		logs.Error("update state fail, err:%s", err.Error())
		return err
	}
	if !pa.attr.isInner {
		headwaters.Cancel(errors.New(errStr))
		headwaters.atlantic.runFail(headwaters, headwaters.atlantic.(AtlanticFlow))
	}
	return nil
}

func (pa *ParallelRiver) setSuccess() error {
	pa.state = stateSuccess
	if err := setFlowEnd(pa.id, stateSuccess.String(), ""); err != nil {
		logs.Error("update state fail, err:%s", err.Error())
		return err
	}
	return nil
}

func (pa *ParallelRiver) setRunning() error {
	pa.state = stateRunning
	if err := setFlowStart(pa.id, stateRunning.String()); err != nil {
		logs.Error("update state fail, err:%s", err.Error())
		return err
	}
	return nil
}

func (pa *ParallelRiver) updateWater(headwaters *Headwaters) error {
	if err := updateHeadwaters(headwaters); err != nil {
		logs.Error("update water fail, err:%s", err.Error())
		return err
	}
	return nil
}

func (pa *ParallelRiver) Run(headwaters *Headwaters, flow RiverFlow, syncNext bool) (err error) {
	defer func() {
		if e := recover(); e != nil {
			logs.Error("%v", e)
			PrintStack()
			_ = pa.setFail("got an panic", headwaters)
			err = errors.New("got an panic")
		}
	}()
	pa.runInit(flow)
	switch pa.state {
	case stateInit:
		if err = pa.setRunning(); err != nil {
			return err
		}
		fallthrough
	case stateRunning:
		if err = pa.runFlow(headwaters, flow); err != nil {
			if err2 := pa.setFail("", headwaters); err2 != nil {
				logs.Error("parallel set fail failed")
			}
			return err
		}
		if pa.attr.Durable {
			if err = pa.updateWater(headwaters); err != nil {
				return err
			}
		}
		if err := pa.setSuccess(); err != nil {
			return err
		}
		fallthrough
	case stateSuccess:
		return pa.runNext(headwaters, syncNext)
	case stateFail:
		logs.Error("flow has been failed")
		err := errors.New("flow has been failed")
		headwaters.Cancel(err)
		return err

	default:
		logs.Error("invalid parallel river state:%s", pa.state.String())
		return errors.New("invalid parallel river state:" + pa.state.String())
	}
}

func (pa *ParallelRiver) next() Stream {
	return pa.down
}

func (pa *ParallelRiver) SetDownStream(down Stream) {
	pa.down = down
}

func (pa *ParallelRiver) setInner() {
	pa.attr.isInner = true
}

func (pa *ParallelRiver) setId(id string) {
	pa.id = id
}

func (pa *ParallelRiver) getId() string {
	return pa.id
}

func (pa *ParallelRiver) setState(state streamState) {
	pa.state = state
}

func (pa *ParallelRiver) getState() streamState {
	return pa.state
}

func (pa *ParallelRiver) setWaterId(waterId string) {
	pa.waterId = waterId
}

func (pa *ParallelRiver) getWaterId() string {
	return pa.waterId
}

func (pa *ParallelRiver) setColor(color string) {
	pa.color = color
	for _, river := range pa.rivers {
		river.setColor(color)
		for r := river.next(); r != nil; r = r.next() {
			r.setColor(color)
		}
	}
}

func (pa *ParallelRiver) GetColor() string {
	return pa.color
}

// used for load
func (pa *ParallelRiver) colorCurrent(color string) {
	pa.color = color
}

func (pa *ParallelRiver) runRiver(headwaters *Headwaters, river Stream) {
	if err := river.Run(headwaters, river.(RiverFlow), true); err != nil && err != ErrorCanceled {
		headwaters.Cancel(err)
	}
	//logs.Debug("one river done")
	pa.wg.Done()
}

func (pa *ParallelRiver) Update(attr *RiverAttr) {
	//logs.Debug("parallel update water ")
}

func (pa *ParallelRiver) Flow(headwaters *Headwaters) (errCause string, err error) {
	logs.Debug("parallel flow water ")
	return "", nil
}

func (pa *ParallelRiver) Cycle(headwaters *Headwaters) (errCause string, err error) {
	logs.Debug("parallel cycle water ")
	return "", nil
}

func (pa *ParallelRiver) Append(river Stream) *ParallelRiver {
	if pa.rivers == nil {
		pa.rivers = make([]Stream, 0)
	}

	if _, ok := river.(*ParallelRiver); ok {
		panic("not supported river")
	}
	river.setInner()
	for r := river.next(); r != nil; r = r.next() {
		r.setInner()
	}
	pa.rivers = append(pa.rivers, river)
	return pa
}

func (pa *ParallelRiver) getRivers() []Stream {
	return pa.rivers
}

func (pa *ParallelRiver) runInit(flow RiverFlow) {
	flow.Update(&pa.attr)
}

func (pa *ParallelRiver) runNext(headwaters *Headwaters, syncNext bool) error {
	// do next
	if pa.next() != nil {
		if syncNext {
			return pa.next().Run(headwaters, pa.next().(RiverFlow), syncNext)
		} else {
			go pa.next().Run(headwaters, pa.next().(RiverFlow), syncNext)
		}
	} else {
		if !pa.attr.isInner {
			headwaters.atlantic.runSuccess(headwaters, headwaters.atlantic.(AtlanticFlow))
		}
	}
	return nil
}

func (pa *ParallelRiver) runFlow(headwaters *Headwaters, flow RiverFlow) error {
	select {
	case <-headwaters.basinFinish():
		return ErrorCanceled
	case <-headwaters.Done():
		return ErrorCanceled
	default:
		//logs.Debug("[%s][%s]run parallel river", headwaters.RequestId, pa.color)
	}
	// do run
	for _, v := range pa.rivers {
		pa.wg.Add(1)
		go pa.runRiver(headwaters, v)
	}
	pa.wg.Wait()

	return headwaters.Err()
}
