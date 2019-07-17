package vastflow

import (
	"github.com/jack0liu/logs"
	"errors"
)

type BasinStream interface {
	Color(color string)
	DrawStream(river Stream) *RiverBasin
	getFirstDream() Stream
}

// every river has own headwaters and flows into atlantic directly
type RiverBasin struct {
	attr RiverAttr

	first Stream
	last  Stream

	id      string
	errStr  string
	down    Stream
	state   streamState
	color   string
	waterId string // used for restore
}

func init() {
	RegisterStream(new(RiverBasin))
}

func (rb *RiverBasin) setFail(errStr string, headwaters *Headwaters) error {
	rb.state = stateFail
	rb.errStr = errStr
	if err := setFlowEnd(rb.id, stateFail.String(), errStr); err != nil {
		logs.Error("update state fail, err:%s", err.Error())
		return err
	}
	if !rb.attr.isInner {
		headwaters.Cancel(errors.New(errStr))
		headwaters.atlantic.runFail(headwaters, headwaters.atlantic.(AtlanticFlow))
	}
	return nil
}

func (rb *RiverBasin) setSuccess() error {
	rb.state = stateSuccess
	if err := setFlowEnd(rb.id, stateSuccess.String(), ""); err != nil {
		logs.Error("update state fail, err:%s", err.Error())
		return err
	}
	return nil
}

func (rb *RiverBasin) setRunning() error {
	rb.state = stateRunning
	if err := setFlowStart(rb.id, stateRunning.String()); err != nil {
		logs.Error("update state fail, err:%s", err.Error())
		return err
	}
	return nil
}

func (rb *RiverBasin) updateWater(headwaters *Headwaters) error {
	if err := updateHeadwaters(headwaters); err != nil {
		logs.Error("update water fail, err:%s", err.Error())
		return err
	}
	return nil
}

func (rb *RiverBasin) Run(headwaters *Headwaters, flow RiverFlow, syncNext bool) (err error) {
	defer func() {
		if e := recover(); e != nil {
			logs.Error("%v", e)
			PrintStack()
			_ = rb.setFail("got an panic", headwaters)
			err = errors.New("got an panic")
		}
	}()
	rb.runInit(flow)
	switch rb.state {
	case stateInit:
		if err = rb.setRunning(); err != nil {
			return err
		}
		fallthrough
	case stateRunning:
		if err = rb.runFlow(headwaters, flow); err != nil {
			if err := rb.setFail("", headwaters); err != nil {
				return err
			}
			return err
		}
		if rb.attr.Durable {
			if err = rb.updateWater(headwaters); err != nil {
				return err
			}
		}
		if err = rb.setSuccess(); err != nil {
			return err
		}
		fallthrough
	case stateSuccess:
		return rb.runNext(headwaters, syncNext)
	case stateFail:
		logs.Error("flow has been failed")
		err = errors.New("flow has been failed")
		headwaters.Cancel(err)
		return err

	default:
		logs.Error("invalid basin state:%s", rb.state.String())
		return errors.New("invalid basin state:" + rb.state.String())
	}
}

func (rb *RiverBasin) next() Stream {
	return rb.down
}

func (rb *RiverBasin) SetDownStream(down Stream) {
	rb.down = down
}

func (rb *RiverBasin) setInner() {
	rb.attr.isInner = true
}

func (rb *RiverBasin) setId(id string) {
	rb.id = id
}

func (rb *RiverBasin) getId() string {
	return rb.id
}

// used for load
func (rb *RiverBasin) colorCurrent(color string) {
	rb.color = color
}

func (rb *RiverBasin) setColor(color string) {
	rb.color = color
	if rb.first == nil {
		return
	}
	rb.first.setColor(color)
	for r := rb.first.next(); r != nil; r = r.next() {
		r.setColor(color)
	}
}

func (rb *RiverBasin) getFirstDream() Stream {
	return rb.first
}

func (rb *RiverBasin) Color(color string) {
	rb.setColor(color)
}

func (rb *RiverBasin) GetColor() string {
	return rb.color
}

func (rb *RiverBasin) setState(state streamState) {
	rb.state = state
}

func (rb *RiverBasin) getState() streamState {
	return rb.state
}

func (rb *RiverBasin) setWaterId(waterId string) {
	rb.waterId = waterId
}

func (rb *RiverBasin) getWaterId() string {
	return rb.waterId
}

func (rb *RiverBasin) runRiver(headwaters *Headwaters, river Stream) {
	if err := river.Run(headwaters, river.(RiverFlow), true); err != nil && err != ErrorBasinCanceled {
		logs.Error(err.Error())
		headwaters.basinCancel(err)
	}
	logs.Debug("basin finish")
}

func (rb *RiverBasin) Update(attr *RiverAttr) {
	logs.Debug("basin update water ")
}

func (rb *RiverBasin) Flow(headwaters *Headwaters) (errCause string, err error) {
	logs.Debug("basin flow water ")
	return "", nil
}

func (rb *RiverBasin) Cycle(headwaters *Headwaters) (errCause string, err error) {
	logs.Debug("basin cycle water ")
	return "", nil
}


func (rb *RiverBasin) DrawStream(river Stream) *RiverBasin {
	if _, ok := river.(RiverFlow); !ok {
		panic("river not implement RiverFlow")
	}

	river.setColor(rb.color)
	river.setInner()

	if rb.first == nil {
		rb.first = river
		rb.last = river
	} else {
		rb.last.SetDownStream(river)
		rb.last = river
	}
	for nxt := river.next(); nxt != nil; nxt = nxt.next() {
		nxt.setInner()
		rb.last.SetDownStream(nxt)
		rb.last = nxt
	}

	return rb
}

func (rb *RiverBasin) runInit(flow RiverFlow) {
	flow.Update(&rb.attr)
}

func (rb *RiverBasin) runNext(headwaters *Headwaters, syncNext bool) error {
	// do next
	if rb.next() != nil {
		if syncNext {
			return rb.next().Run(headwaters, rb.next().(RiverFlow), syncNext)
		} else {
			go rb.next().Run(headwaters, rb.next().(RiverFlow), syncNext)
		}
	} else {
		if !rb.attr.isInner {
			headwaters.atlantic.runSuccess(headwaters, headwaters.atlantic.(AtlanticFlow))
		}
	}
	return nil
}

func (rb *RiverBasin) runFlow(headwaters *Headwaters, flow RiverFlow) error {
	select {
	case <-headwaters.basinFinish():
		return ErrorCanceled
	case <-headwaters.Done():
		return ErrorCanceled
	default:
		logs.Debug("run basin river")
	}
	// do run
	if rb.first == nil {
		logs.Error("no river in basin")
		return errors.New("no river in basin")
	}
	logs.Debug("headwaters id:%s", rb.first.getWaterId())
	var newHeadwaters *Headwaters
	fw := queryWaterById(rb.first.getWaterId())
	if fw == nil {
		logs.Error("no flow water")
		return errors.New("no flow water")
	}
	if rb.isWaterUpdated(fw) {
		newHeadwaters = fromPersistWater(fw.Headwaters)

		// replace global and basin context
		newHeadwaters.atlantic = headwaters.atlantic
		newHeadwaters.basinDone = headwaters.basinDone
		newHeadwaters.basinMu = headwaters.basinMu
		newHeadwaters.basinErr = headwaters.basinErr
	} else {
		newHeadwaters = headwaters.copy4Basin()
		newHeadwaters.id = rb.first.getWaterId()
	}
	logs.Debug("new basin(%s) headwaters id:%s", rb.color, newHeadwaters.id)
	rb.runRiver(newHeadwaters, rb.first)
	headwaters.Replace(newHeadwaters)
	return headwaters.basinError()
}

func (rb *RiverBasin) isWaterUpdated(fw *FlowWater) bool {
	if len(fw.UpdateAt) == 0 {
		return false
	}
	return true
}
