package vastflow

import (
	"github.com/astaxie/beego/orm"
	"github.com/jack0liu/logs"
	"github.com/jack0liu/utils"
)

const (
	FlowAction  = "vast_flow_action"
	FlowObject  = "vast_flow_object"
	FlowProject = "vast_flow_project"

	flowTypeCommon   = "common"
	flowTypeParallel = "multi"
	flowTypeBasin    = "basin"
	flowTypeAtlantic = "atlantic"

	rootParent = "andes"
)

type persistErrorBasin struct {
	Err string
}

type persistWater struct {
	RequestId string
	ReqInfo   interface{} // request info from andes
	Atlantic  string

	// basin context
	BasinDone bool // must comes from basin
	BasinErr  *persistErrorBasin

	// river context
	Id      string
	Context map[string]interface{} // water's context
	Done    bool
	Err     string // set to non-nil by the first cancel call
}

type FlowWater struct {
	Id         string `orm:"size(64);pk"`
	RequestId  string `orm:"size(64)"`
	Headwaters string `orm:"null;type(text)"`
	UpdateAt   string `orm:"null;size(64)"`
	Deleted    int    `orm:"default(0)"`
}

type VastFlow struct {
	Id        string `orm:"size(64);pk"`
	ParentId  string `orm:"null;size(64)"`
	RequestId string `orm:"size(64)"`
	Index     int    `orm:"default(0)"`
	FlowType  string `orm:"size(64)"`
	Name      string `orm:"size(64)"`
	ObjName   string `orm:"size(128)"`
	ProjectId string `orm:"size(64)"`
	Action    string `orm:"size(64)"`
	State     string `orm:"null;size(64)"`
	BeginAt   string `orm:"null;size(64)"`
	EndAt     string `orm:"null;size(64)"`
	Color     string `orm:"null;size(128)"`
	Error     string `orm:"null;type(text)"`
	WaterId   string `orm:"size(64)"`
	Deleted   int    `orm:"default(0)"`
}

func init() {
	// register model
	orm.RegisterModel(new(FlowWater))
	orm.RegisterModel(new(VastFlow))
}

func updateFlowState(flowId string, state string) error {
	o := orm.NewOrm()
	flow := VastFlow{
		Id:    flowId,
		State: state,
	}
	if _, err := o.Update(&flow, "state"); err != nil {
		return err
	}
	return nil
}

func setFlowEnd(flowId string, state string, err string) error {
	o := orm.NewOrm()
	flow := VastFlow{
		Id:    flowId,
		State: state,
		EndAt: utils.GetCurrentTime(),
		Error: err,
	}
	if _, err := o.Update(&flow, "state", "end_at", "error"); err != nil {
		return err
	}
	return nil
}

func setFlowStart(flowId string, state string) error {
	o := orm.NewOrm()
	flow := VastFlow{
		Id:      flowId,
		State:   state,
		BeginAt: utils.GetCurrentTime(),
	}
	if _, err := o.Update(&flow, "state", "begin_at"); err != nil {
		return err
	}
	return nil
}

func updateHeadwaters(headwaters *Headwaters) error {
	o := orm.NewOrm()
	water := FlowWater{
		Id:         headwaters.id,
		Headwaters: toPersistWater(headwaters),
		UpdateAt:   utils.GetCurrentTime(),
	}
	if _, err := o.Update(&water, "headwaters", "update_at"); err != nil {
		return err
	}
	return nil
}

func queryFlowById(flowId string) *VastFlow {
	o := orm.NewOrm()
	vf := VastFlow{Id: flowId}
	if err := o.Read(&vf); err != nil {
		logs.Error("can't find andes flowId:%s", flowId)
		return nil
	}
	return &vf
}

func queryRootFlowByRequestId(requestId string) *VastFlow {
	var flows []*VastFlow
	o := orm.NewOrm()
	qs := o.QueryTable(new(VastFlow))
	qs = qs.Filter("request_id", requestId)
	qs = qs.Filter("parent_id", rootParent)
	if _, err := qs.All(&flows); err != nil {
		logs.Error("query flows fail, parentId:%s, requestId:%s", rootParent, requestId)
		return nil
	}
	if len(flows) == 0 {
		logs.Debug("not found flow, parentId:%s, requestId:%s", rootParent, requestId)
		return nil
	}
	return flows[0]
}

func queryWaterById(waterId string) *FlowWater {
	o := orm.NewOrm()
	fw := FlowWater{Id: waterId}
	if err := o.Read(&fw); err != nil {
		logs.Error("can't find water waterId:%s", waterId)
		return nil
	}
	return &fw
}

func queryFlowByParentIdAndType(parentId, flowType string) []*VastFlow {
	var flows []*VastFlow
	o := orm.NewOrm()
	qs := o.QueryTable(new(VastFlow))
	qs = qs.Filter("deleted", false) // used index
	qs = qs.Filter("parent_id", parentId)
	qs = qs.Filter("flow_type", flowType)
	if _, err := qs.All(&flows); err != nil {
		logs.Error("query flows fail, parentId:%s, flowType:%s", parentId, flowType)
	}
	return flows
}

func queryAtlanticByRequestId(requestId string) []*VastFlow {
	var flows []*VastFlow
	o := orm.NewOrm()
	qs := o.QueryTable(new(VastFlow))
	qs = qs.Filter("deleted", false) // used index
	qs = qs.Filter("request_id", requestId)
	qs = qs.Filter("flow_type", flowTypeAtlantic)
	if _, err := qs.All(&flows); err != nil {
		logs.Error("query atlantic fail, requestId:%s", requestId)
	}
	return flows
}

func queryFlowByParentIdAndIndex(parentId string, index int) []*VastFlow {
	var flows []*VastFlow
	o := orm.NewOrm()
	qs := o.QueryTable(new(VastFlow))
	qs = qs.Filter("deleted", false) // used index
	qs = qs.Filter("parent_id", parentId)
	qs = qs.Filter("index", index)
	if _, err := qs.All(&flows); err != nil {
		logs.Error("query flows fail, parentId:%s", parentId)
	}
	return flows
}
