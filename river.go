package vastflow

import (
	"github.com/jack0liu/logs"
	"errors"
	"fmt"
	"reflect"
	"time"
)

var (
	ErrorRetry         = errors.New("river retries to run")
	ErrorCanceled      = errors.New("river canceled to run")
	ErrorBasinCanceled = errors.New("basin canceled to run")
	ErrorContinue      = errors.New("river needs to continue")
)

type RiverFlow interface {
	Update(attr *RiverAttr)
	Flow(headwaters *Headwaters) (errCause string, err error)
	Cycle(headwaters *Headwaters) (errCause string, err error)
}

type RiverAttr struct {
	RetryTimes int32
	//seconds
	RetryInterval int32

	CycleTimes int32
	//seconds
	CycleInterval int32

	Durable bool

	Atomic bool

	isInner bool
}

type River struct {
	attr RiverAttr

	down    Stream
	state   streamState
	errStr  string // failed error str
	id      string
	color   string
	waterId string // used to restore flow

	retryCount int32
	cycleCount int32
}

func (an *River) setFail(errStr string, flow RiverFlow, headwaters *Headwaters) error {
	logs.Info("[%s][%s]fail", headwaters.RequestId, an.color)
	an.state = stateFail
	an.errStr = errStr
	cause := fmt.Sprintf("%v:%s", reflect.ValueOf(flow).Elem().Type(), errStr)
	if err := setFlowEnd(an.id, stateFail.String(), cause); err != nil {
		logs.Error("update state fail, err:%s", err.Error())
		return err
	}
	if !an.attr.isInner {
		headwaters.Cancel(errors.New(errStr))
		headwaters.atlantic.runFail(headwaters, headwaters.atlantic.(AtlanticFlow))
	}
	return errors.New(cause)
}

func (an *River) setSuccess() error {
	an.state = stateSuccess
	if err := setFlowEnd(an.id, stateSuccess.String(), ""); err != nil {
		logs.Error("update state fail, err:%s", err.Error())
		return err
	}
	return nil
}

func (an *River) setRunning() error {
	an.state = stateRunning
	if err := setFlowStart(an.id, stateRunning.String()); err != nil {
		logs.Error("update state fail, err:%s", err.Error())
		return err
	}
	return nil
}

func (an *River) setCycling() error {
	an.state = stateCycling
	if err := updateFlowState(an.id, stateCycling.String()); err != nil {
		logs.Error("update state fail, err:%s", err.Error())
		return err
	}
	return nil
}

func (an *River) updateWater(headwaters *Headwaters) error {
	if err := updateHeadwaters(headwaters); err != nil {
		logs.Error("update water fail, err:%s", err.Error())
		return err
	}
	return nil
}

func (an *River) innerFlow(headwaters *Headwaters, flow RiverFlow) (errCause string, err error) {
	select {
	case <-headwaters.basinFinish():
		if an.attr.Atomic {
			return flow.Flow(headwaters)
		} else {
			return "", ErrorBasinCanceled
		}
	case <-headwaters.Done():
		if an.attr.Atomic {
			return flow.Flow(headwaters)
		} else {
			return "", ErrorCanceled
		}
	default:
		return flow.Flow(headwaters)
	}
}

func (an *River) Run(headwaters *Headwaters, flow RiverFlow, syncNext bool) (err error) {
	logs.Info("[%s][%s]%v run start, id :%s", headwaters.RequestId, an.color, reflect.ValueOf(flow).Elem().Type(), an.id)
	defer func() {
		if e := recover(); e != nil {
			logs.Error("[%s][%s]%v", headwaters.RequestId, an.color, e)
			PrintStack()
			_ = an.setFail("got an panic", flow, headwaters)
			err = errors.New("got an panic")
		}
	}()
	an.runInit(flow)
	switch an.state {
	case stateInit:
		if err = an.setRunning(); err != nil {
			return err
		}
		fallthrough
	case stateRunning:
		if err = an.runFlow(headwaters, flow, syncNext); err != nil {
			if err := an.setFail(err.Error(), flow, headwaters); err != nil {
				return err
			}
			return err
		}
		if an.attr.Durable {
			if err := an.updateWater(headwaters); err != nil {
				return err
			}
		}
		if an.attr.CycleTimes <= 0 {
			// no cycle, set success , then do next
			if err = an.setSuccess(); err != nil {
				return err
			}
			return an.runNext(headwaters, syncNext)
		}
		// do cycle
		if err = an.setCycling(); err != nil {
			return err
		}
		fallthrough
	case stateCycling:
		if err = an.runCycle(headwaters, flow); err != nil {
			if err := an.setFail(err.Error(), flow, headwaters); err != nil {
				return err
			}
			return err
		}
		if an.attr.Durable {
			if err = an.updateWater(headwaters); err != nil {
				return err
			}
		}
		if err = an.setSuccess(); err != nil {
			return err
		}
		fallthrough
	case stateSuccess:
		logs.Info("[%s][%s]%v run success, id :%s", headwaters.RequestId, an.color, reflect.ValueOf(flow).Elem().Type(), an.id)
		return an.runNext(headwaters, syncNext)
	case stateFail:
		logs.Error("[%s][%s]flow has been failed", headwaters.RequestId, an.color)
		err = errors.New("flow has been failed")
		headwaters.Cancel(err)
		return err

	default:
		logs.Error("invalid river state:%s", an.state.String())
		return errors.New("invalid river state:" + an.state.String())
	}
}

func (an *River) next() Stream {
	return an.down
}

func (an *River) setInner() {
	an.attr.isInner = true
}

func (an *River) SetDownStream(down Stream) {
	an.down = down
}

func (an *River) setId(id string) {
	an.id = id
}

func (an *River) getId() string {
	return an.id
}

func (an *River) setColor(color string) {
	an.color = color
}

// used for load
func (an *River) colorCurrent(color string) {
	an.color = color
}

func (an *River) GetColor() string {
	return an.color
}

func (an *River) setState(state streamState) {
	an.state = state
}

func (an *River) getState() streamState {
	return an.state
}

func (an *River) setWaterId(waterId string) {
	an.waterId = waterId
}

func (an *River) getWaterId() string {
	return an.waterId
}

func (an *River) doRetry(headwaters *Headwaters, flow RiverFlow) (errStr string, err error) {
	var eStr string
	for an.retryCount < an.attr.RetryTimes {
		logs.Debug("[%s][%s]retry count : %d", headwaters.RequestId, an.color, an.retryCount)
		time.Sleep(time.Duration(an.attr.RetryInterval) * time.Second)
		eStr, err = an.innerFlow(headwaters, flow)
		if err != nil {
			an.retryCount++
			if err == ErrorRetry {
				continue
			} else {
				return eStr, err
			}
		} else {
			return "", nil
		}
	}
	errStr = fmt.Sprintf("[%s][%s]retry %d times failed, cause:%s", headwaters.RequestId, an.color, an.retryCount, eStr)
	return errStr, errors.New("retry timeout")
}

func (an *River) doCycle(headwaters *Headwaters, flow RiverFlow) (errStr string, err error) {
	if err := an.setCycling(); err != nil {
		return err.Error(), err
	}
	for an.cycleCount < an.attr.CycleTimes {
		time.Sleep(time.Duration(an.attr.CycleInterval) * time.Second)
		select {
		case <-headwaters.basinFinish():
			if an.attr.Atomic {
				errStr, err = flow.Cycle(headwaters)
			} else {
				return "basin canceled", ErrorCanceled
			}
		case <-headwaters.Done():
			if an.attr.Atomic {
				errStr, err = flow.Cycle(headwaters)
			} else {
				return "river canceled", ErrorCanceled
			}
		default:
			errStr, err = flow.Cycle(headwaters)
		}
		if err == nil {
			//out set success
			return "", nil
		}
		if err == ErrorContinue {
			an.cycleCount++
			continue
		}
		return err.Error(), err
	}
	errStr = fmt.Sprintf("[%s][%s]cycle %d times failed, cause:%s", headwaters.RequestId, an.color, an.cycleCount, errStr)
	return errStr, errors.New(errStr)
}

func (an *River) runInit(flow RiverFlow) {
	flow.Update(&an.attr)
}

func (an *River) runNext(headwaters *Headwaters, syncNext bool) error {
	// do next
	if an.next() != nil {
		if syncNext {
			return an.next().Run(headwaters, an.next().(RiverFlow), syncNext)
		} else {
			go an.next().Run(headwaters, an.next().(RiverFlow), syncNext)
		}
	} else {
		if !an.attr.isInner {
			headwaters.atlantic.runSuccess(headwaters, headwaters.atlantic.(AtlanticFlow))
		}
	}
	return nil
}

func (an *River) runCycle(headwaters *Headwaters, flow RiverFlow) error {
	var errStr string
	var err error
	for an.cycleCount < an.attr.CycleTimes {
		time.Sleep(time.Duration(an.attr.CycleInterval) * time.Second)
		select {
		case <-headwaters.basinFinish():
			if an.attr.Atomic {
				errStr, err = flow.Cycle(headwaters)
			} else {
				return ErrorCanceled
			}
		case <-headwaters.Done():
			if an.attr.Atomic {
				errStr, err = flow.Cycle(headwaters)
			} else {
				return ErrorCanceled
			}
		default:
			errStr, err = flow.Cycle(headwaters)
		}
		if err == nil {
			//out set success
			logs.Info("[%s][%s]%v cycle success",headwaters.RequestId, an.color, reflect.ValueOf(flow).Elem().Type())
			return nil
		}
		if err == ErrorContinue {
			an.cycleCount++
			continue
		}
		logs.Info("[%s][%s]%v cycle err:%s",headwaters.RequestId, an.color, reflect.ValueOf(flow).Elem().Type(), err.Error())
		return err
	}
	errStr = fmt.Sprintf("[%s][%s]cycle %d times failed, cause:%s", headwaters.RequestId, an.color, an.cycleCount, errStr)
	return errors.New(errStr)
}

func (an *River) runFlow(headwaters *Headwaters, flow RiverFlow, b bool) error {
	// do run
	errStr, err := an.innerFlow(headwaters, flow)
	if err == nil {
		return nil
	}
	if err != ErrorRetry {
		logs.Error("[%s][%s]%v run failed, err: %s", headwaters.RequestId, an.color, reflect.ValueOf(flow).Elem().Type(), err.Error())
		return err
	}
	if an.attr.RetryTimes <= 0 {
		return errors.New(errStr)
	}
	errStr, err = an.doRetry(headwaters, flow)
	if err != nil {
		return errors.New(errStr)
	}
	return nil
}
