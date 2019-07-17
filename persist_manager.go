package vastflow

import (
	"encoding/json"
	"errors"
	"github.com/astaxie/beego/orm"
 	"github.com/jack0liu/logs"
	"github.com/satori/go.uuid"
	"reflect"
	"sync"
)

func saveDraw(andes *Andes, initState streamState) (andesId string, err error) {
	requestId := andes.headwaters.RequestId
	rivers := make([]Stream, 0)
	rivers = append(rivers, andes.first)

	waters := make([]*FlowWater, 0)
	flows := make([]*VastFlow, 0)
	waters, flows = buildFlows(waters, flows, rivers, requestId, rootParent, initState, 0, andes.headwaters)
	flows = buildAtlantic(flows, initState, andes.headwaters)
	logs.Debug("len(flows):%d", len(flows))
	if len(flows) == 0 {
		err = errors.New("flows length is 0")
		logs.Error("flows length is 0")
		return "", err
	}

	// db insert with transaction
	o := orm.NewOrm()
	if err := o.Begin(); err != nil {
		logs.Error("db begin transaction fail")
		return "", err
	}
	if _, err := o.InsertMulti(10, waters); err != nil {
		logs.Error("saveDraw waters error")
		o.Rollback()
		return "", err
	}
	if _, err := o.InsertMulti(20, flows); err != nil {
		logs.Error("saveDraw flows error")
		o.Rollback()
		return "", err
	}
	if err := o.Commit(); err != nil {
		logs.Error("db commit transaction fail")
		return "", err
	}
	return flows[0].Id, nil
}

func buildAtlantic(flows []*VastFlow, initState streamState, headwaters *Headwaters) []*VastFlow {
	at := headwaters.atlantic
	atlanticName := reflect.TypeOf(at).Elem().Name()
	objName, action, projectId := getFlowLocation(headwaters)

	vf := &VastFlow{
		Id:        uuid.NewV4().String(),
		RequestId: headwaters.RequestId,
		ParentId:  "headwaters",
		Index:     -1,
		FlowType:  "atlantic",
		Name:      atlanticName,
		ObjName:   objName,
		ProjectId: projectId,
		Action:    action,
		Color:     "",
		State:     initState.String(),
		WaterId:   headwaters.id,
	}
	at.setId(vf.Id)
	flows = append(flows, vf)
	return flows
}

func getFlowLocation(headwaters *Headwaters) (objName, action, projectId string) {
	if obj, ok := headwaters.Get(FlowObject).(string); ok {
		objName = obj
	}
	if ac, ok := headwaters.Get(FlowAction).(string); ok {
		action = ac
	}
	if pr, ok := headwaters.Get(FlowProject).(string); ok {
		projectId = pr
	}
	return objName, action, projectId
}

func buildFlows(waters []*FlowWater, flows []*VastFlow, rivers []Stream, requestId, parentId string, initState streamState,
	startIndex int, headwaters *Headwaters) (outWaters []*FlowWater, outFlows []*VastFlow) {
	for _, v := range rivers {
		currentId := uuid.NewV4().String()
		riverName := reflect.TypeOf(v).Elem().Name()

		objName, action, projectId := getFlowLocation(headwaters)

		if len(headwaters.id) == 0 {
			hwId := uuid.NewV4().String()
			headwaters.id = hwId
			fw := &FlowWater{
				Id:         hwId,
				RequestId:  headwaters.RequestId,
				Headwaters: toPersistWater(headwaters),
			}
			waters = append(waters, fw)
			logs.Debug("water requestId:%v", fw.RequestId)
		}
		switch v.(type) {
		case *ParallelRiver:
			vf := &VastFlow{
				Id:        currentId,
				RequestId: requestId,
				ParentId:  parentId,
				Index:     startIndex,
				FlowType:  flowTypeParallel,
				Name:      riverName,
				ObjName:   objName,
				ProjectId: projectId,
				Action:    action,
				Color:     v.GetColor(),
				State:     initState.String(),
				WaterId:   headwaters.id,
			}
			v.setId(vf.Id)
			v.setWaterId(vf.WaterId)
			v.setState(initState)
			//logs.Debug("%v", vf)
			flows = append(flows, vf)
			pr := v.(*ParallelRiver)
			waters, flows = buildFlows(waters, flows, pr.rivers, requestId, vf.Id, initState, 0, headwaters)
		case *RiverBasin:
			rb := v.(*RiverBasin)
			vf := &VastFlow{
				Id:        currentId,
				RequestId: requestId,
				ParentId:  parentId,
				Index:     startIndex,
				FlowType:  flowTypeBasin,
				Name:      riverName,
				ObjName:   objName,
				ProjectId: projectId,
				Action:    action,
				Color:     v.GetColor(),
				State:     initState.String(),
				WaterId:   headwaters.id,
			}
			v.setId(vf.Id)
			v.setWaterId(vf.WaterId)
			v.setState(initState)
			flows = append(flows, vf)
			rivers := make([]Stream, 0)
			rivers = append(rivers, rb.first)
			newWater := headwaters.copy4Basin()
			fw := &FlowWater{
				Id:         newWater.id,
				RequestId:  newWater.RequestId,
				Headwaters: toPersistWater(newWater),
			}
			waters = append(waters, fw)
			logs.Debug("water:%v", fw)
			waters, flows = buildFlows(waters, flows, rivers, requestId, vf.Id, initState, 0, newWater)

		default:
			vf := &VastFlow{
				Id:        currentId,
				RequestId: requestId,
				ParentId:  parentId,
				Index:     startIndex,
				FlowType:  flowTypeCommon,
				Name:      riverName,
				ObjName:   objName,
				ProjectId: projectId,
				Action:    action,
				Color:     v.GetColor(),
				State:     initState.String(),
				WaterId:   headwaters.id,
			}
			v.setId(vf.Id)
			v.setWaterId(vf.WaterId)
			v.setState(initState)
			//logs.Debug("%v", vf)
			flows = append(flows, vf)
		}

		if nxt := v.next(); nxt != nil {
			nextRivers := make([]Stream, 0)
			nextRivers = append(nextRivers, nxt)
			waters, flows = buildFlows(waters, flows, nextRivers, requestId, currentId, initState, startIndex+1, headwaters)
		}
	}
	return waters, flows
}

func toPersistWater(headwaters *Headwaters) string {
	if headwaters == nil {
		return ""
	}
	basinDone := false
	select {
	case _, ok := <-headwaters.basinFinish():
		if !ok {
			basinDone = true
		}
	default:
	}

	waterDone := false
	select {
	case _, ok := <-headwaters.Done():
		if !ok {
			waterDone = true
		}
	default:
	}

	var basicErr *persistErrorBasin
	if headwaters.basinErr != nil && headwaters.basinErr.err != nil {
		basicErr = &persistErrorBasin{
			Err: headwaters.basinErr.err.Error(),
		}
	}

	var errStr string
	if headwaters.Err() != nil {
		errStr = headwaters.Err().Error()
	}

	atlanticName := reflect.TypeOf(headwaters.atlantic).Elem().Name()

	pw := persistWater{
		RequestId: headwaters.RequestId,
		ReqInfo:   headwaters.ReqInfo,
		Atlantic:  atlanticName,

		BasinDone: basinDone,
		BasinErr:  basicErr,

		// river context
		Id:      headwaters.id,
		Context: headwaters.context,
		Done:    waterDone,
		Err:     errStr,
	}
	headwaters.mu.RLock()
	waterStr, err := json.Marshal(&pw)
	if err != nil {
		logs.Error("headwaters transfer persist water fail , err:%s", err.Error())
	}
	headwaters.mu.RUnlock()
	return string(waterStr)
}

func fromPersistWater(waterStr string) *Headwaters {
	if len(waterStr) == 0 {
		logs.Info("empty water skip")
		return nil
	}
	var pw persistWater
	if err := json.Unmarshal([]byte(waterStr), &pw); err != nil {
		logs.Error("water str transfer persistErrorBasin fail, err:%s", err.Error())
		return nil
	}
	stream := getAtlantic(pw.Atlantic)
	if stream == nil {
		logs.Error("water cant't find atlantic:%s", pw.Atlantic)
		return nil
	}
	iStream := reflect.New(stream).Interface()
	atlantic, ok := iStream.(AtlanticStream)
	if !ok {
		logs.Error("it's not a atlantic, name: %s", pw.Atlantic)
		return nil
	}

	var headwaters Headwaters
	headwaters.RequestId = pw.RequestId
	headwaters.ReqInfo = pw.ReqInfo
	headwaters.atlantic = atlantic

	// basin
	headwaters.basinMu = &sync.Mutex{}
	if pw.BasinErr != nil && len(pw.BasinErr.Err) > 0 {
		headwaters.basinErr = &errorBasin{
			err: errors.New(pw.BasinErr.Err),
		}
	} else {
	 	headwaters.basinErr = &errorBasin{}
	}
	if pw.BasinDone {
		headwaters.basinDone = closedChan
	} else {
		headwaters.basinDone = make(chan struct{})
	}

	headwaters.id = pw.Id
	headwaters.context = pw.Context
	if pw.Done {
		headwaters.done = closedChan
	} else {
		headwaters.done = make(chan struct{})
	}
	if len(pw.Err) > 0 {
		headwaters.err = errors.New(pw.Err)
	}

	return &headwaters
}

func LoadAndesByRequestId(requestId string) *Andes {
	flow := queryRootFlowByRequestId(requestId)
	if flow == nil {
		logs.Debug("not found requestId(%s) flow", requestId)
		return nil
	}
	logs.Debug("found requestId(%s) flow", requestId)
	return load(flow)
}

func LoadAndes(flowId string) *Andes {
	vf := queryFlowById(flowId)
	if vf == nil {
		return nil
	}
	return load(vf)
}

func load(vf *VastFlow) *Andes {
	fw := queryWaterById(vf.WaterId)
	if fw == nil {
		return nil
	}
	if vf.ParentId != rootParent {
		logs.Error("invalid andes flow:%s, parentId is:%s", vf.Id, vf.ParentId)
		return nil
	}
	logs.Debug("load andes(%s) start", vf.Id)

	// load atlantic and water
	headwaters := fromPersistWater(fw.Headwaters)
	atlantic := loadAtlantic(vf)
	// load flow stream
	stream, err := loadStream(vf, nil)
	if err != nil {
		return nil
	}

	andes := &Andes{}
	andes.DrawStream(stream)
	andes.DrawHeadWaters(headwaters)
	andes.DrawAtlantic(atlantic)
	logs.Debug("load andes(%s) end", vf.Id)
	return andes
}

func loadAtlantic(flow *VastFlow) AtlanticStream {
	flows := queryAtlanticByRequestId(flow.RequestId)
	if len(flows) == 0 {
		logs.Error("not found atlantic, requestId:%s", flow.RequestId)
		return nil
	}
	atlanticFlow := flows[0]
	stream := getAtlantic(atlanticFlow.Name)
	if stream == nil {
		logs.Error("cant't find atlantic:%s", atlanticFlow.Name)
		return nil
	}
	iStream := reflect.New(stream).Interface()
	atlantic, ok := iStream.(AtlanticStream)
	if !ok {
		logs.Error("it's not a atlantic, name: %s", atlanticFlow.Name)
		return nil
	}
	setAtlanticInfo(atlantic, atlanticFlow)
	return iStream.(AtlanticStream)
}

func loadStream(flow *VastFlow, parentStream Stream) (out Stream, err error) {
	stream := getStream(flow.Name)
	if stream == nil {
		logs.Error("cant't find flow:%s", flow.Name)
		return nil, errors.New("missed flow:" + flow.Name)
	}
	iStream := reflect.New(stream).Interface()
	switch flow.FlowType {
	case flowTypeCommon:
		logs.Debug("load common river:%s, color:%s", flow.Name, flow.Color)
		curStream, ok := iStream.(Stream)
		if !ok {
			logs.Error("new flow is not a Stream, name: %s", flow.Name)
			return nil, errors.New("missed flow:" + flow.Name)
		}

		if parentStream != nil && flow.Index > 0 {
			parentStream.SetDownStream(curStream)
		}
		setRiverInfo(curStream, flow)
		nextFlows := queryFlowByParentIdAndIndex(flow.Id, flow.Index+1)
		if len(nextFlows) > 0 {
			// only one next flow
			next := nextFlows[0]
			st, err := loadStream(next, curStream)
			if err != nil {
				return nil, err
			}
			curStream.SetDownStream(st)
		}
		return curStream, nil
	case flowTypeParallel:
		logs.Debug("load parallel river:%s, color:%s", flow.Name, flow.Color)
		curStream, ok := iStream.(Stream)
		if !ok {
			logs.Error("new flow is not a Stream, name: %s", flow.Name)
			return nil, errors.New("missed flow:" + flow.Name)
		}
		parallel, ok := iStream.(ParallelStream)
		if !ok {
			logs.Error("new flow is not a ParallelRiver, name: %s", flow.Name)
			return nil, errors.New("missed flow:" + flow.Name)
		}
		if parentStream != nil && flow.Index > 0 {
			parentStream.SetDownStream(curStream)
		}

		flows := queryFlowByParentIdAndIndex(flow.Id, 0)
		for _, f := range flows {
			st, err := loadStream(f, curStream)
			if err != nil {
				return nil, err
			}
			parallel.Append(st)
		}
		nextFlows := queryFlowByParentIdAndIndex(flow.Id, flow.Index+1)
		if len(nextFlows) > 0 {
			next := nextFlows[0]
			st, err := loadStream(next, curStream)
			if err != nil {
				return nil, err
			}
			curStream.SetDownStream(st)
		}

		setRiverInfo(curStream, flow)
		return curStream, nil
	case flowTypeBasin:
		logs.Debug("load basin river:%s, color:%s", flow.Name, flow.Color)
		curStream := iStream.(Stream)
		basin, ok := iStream.(BasinStream)
		if !ok {
			logs.Error("new flow is not a RiverBasin, name: %s", flow.Name)
			return nil, errors.New("missed flow:" + flow.Name)
		}
		if parentStream != nil && flow.Index > 0 {
			parentStream.SetDownStream(curStream)
		}

		// sub stream
		flows := queryFlowByParentIdAndIndex(flow.Id, 0)
		for _, f := range flows {
			st, err := loadStream(f, curStream)
			if err != nil {
				return nil, err
			}
			basin.DrawStream(st)
		}

		// next stream
		nextFlows := queryFlowByParentIdAndIndex(flow.Id, flow.Index+1)
		if len(nextFlows) > 0 {
			next := nextFlows[0]
			st, err := loadStream(next, curStream)
			if err != nil {
				return nil, err
			}
			curStream.SetDownStream(st)
		}

		setRiverInfo(curStream, flow)
		return curStream, nil
	default:
		logs.Error("invalid flow name:%s", flow.Name)
		return nil, errors.New("invalid flow name:" + flow.Name)
	}

}

func setRiverInfo(s Stream, flow *VastFlow) {
	s.setId(flow.Id)
	s.colorCurrent(flow.Color)
	s.setState(stateMap[flow.State])
	s.setWaterId(flow.WaterId)
}

func setAtlanticInfo(a AtlanticStream, flow *VastFlow) {
	a.setId(flow.Id)
	a.setState(stateMap[flow.State])
	a.setWaterId(flow.WaterId)
}
