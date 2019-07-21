package vastflow

import (
	"github.com/jack0liu/logs"
	"reflect"
)

type AtlanticFlow interface {
	Success(headwaters *Headwaters) error
	Fail(headwaters *Headwaters) error
}

type AtlanticStream interface {
	runSuccess(headwaters *Headwaters, flow AtlanticFlow)
	runFail(headwaters *Headwaters, flow AtlanticFlow)
	setId(id string)
	getId() string
	setState(state streamState)
	getState() streamState
	setWaterId(waterId string)
	getWaterId() string
}

type Atlantic struct {
	id      string
	state   streamState
	waterId string // used to restore flow
}

func (at *Atlantic) updateWater(headwaters *Headwaters) error {
	if err := updateHeadwaters(headwaters); err != nil {
		logs.Error("update water fail, err:%s", err.Error())
		return err
	}
	return nil
}

func (at *Atlantic) runSuccess(headwaters *Headwaters, flow AtlanticFlow) {
	logs.Debug("%v run , id :%s", reflect.ValueOf(flow).Elem().Type(), at.id)
	defer func() {
		if e := recover(); e != nil {
			logs.Error("%v", e)
			PrintStack()
			_ = setFlowEnd(at.id, stateFail.String(), "got an panic")
		}
	}()
	at.releaseWnd()
	switch at.state {
	case stateInit:
		if err := setFlowStart(at.id, stateRunning.String()); err != nil {
			logs.Error("update state fail, err:%s", err.Error())
			return
		}
		fallthrough
	case stateRunning:
		job, _ := GetJobByRequestId(headwaters.RequestId)
		if job != nil {
			_ = UpdateJobStatus(job.Id, JobSuccess)
		}
		if err := flow.Success(headwaters); err != nil {
			if err := setFlowEnd(at.id, stateFail.String(), err.Error()); err != nil {
				logs.Error("update state fail, err:%s", err.Error())
				return
			}
		}
		if err := at.updateWater(headwaters); err != nil {
			logs.Warn("update water fail, err:%s", err.Error())
		}
		if err := setFlowEnd(at.id, stateSuccess.String(), ""); err != nil {
			logs.Error("update state fail, err:%s", err.Error())
			return
		}
		fallthrough
	case stateSuccess:
		logs.Info("[%s]run to atlantic success", headwaters.RequestId)
	default:
		logs.Error("invalid basin state:%s", at.state.String())
	}
}

func (at *Atlantic) runFail(headwaters *Headwaters, flow AtlanticFlow) {
	logs.Debug("%v run , id :%s, state:%s", reflect.ValueOf(flow).Elem().Type(), at.id, at.state.String())
	defer func() {
		if e := recover(); e != nil {
			logs.Error("%v", e)
			PrintStack()
			_ = setFlowEnd(at.id, stateFail.String(), "got an panic")
		}
	}()
	at.releaseWnd()
	switch at.state {
	case stateInit:
		if err := setFlowStart(at.id, stateRunning.String()); err != nil {
			logs.Error("update state fail, err:%s", err.Error())
			return
		}
		fallthrough
	case stateRunning:
		job, _ := GetJobByRequestId(headwaters.RequestId)
		if job != nil {
			_ = UpdateJobStatus(job.Id, JobFailed)
		}

		if err := flow.Fail(headwaters); err != nil {
			if err := setFlowEnd(at.id, stateFail.String(), err.Error()); err != nil {
				logs.Error("update state fail, err:%s", err.Error())
			}
		}
		if err := at.updateWater(headwaters); err != nil {
			logs.Warn("update water fail, err:%s", err.Error())
		}
		if err := setFlowEnd(at.id, stateFail.String(), ""); err != nil {
			logs.Error("update state fail, err:%s", err.Error())
			return
		}
		fallthrough
	case stateFail:
		logs.Info("[%s]run to atlantic failed", headwaters.RequestId)
	default:
		logs.Error("invalid basin state:%s", at.state.String())
	}
}

func (at *Atlantic) setId(id string) {
	at.id = id
}

func (at *Atlantic) getId() string {
	return at.id
}

func (at *Atlantic) setState(state streamState) {
	at.state = state
}

func (at *Atlantic) getState() streamState {
	return at.state
}

func (at *Atlantic) setWaterId(waterId string) {
	at.waterId = waterId
}

func (at *Atlantic) getWaterId() string {
	return at.waterId
}

func (at *Atlantic) releaseWnd() {
	flowWnd.Lock()
	flowWnd.Dec()
	flowWnd.Unlock()
}
