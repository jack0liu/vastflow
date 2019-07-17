package vastflow

import (
	"github.com/jack0liu/logs"
	"errors"
	"reflect"
)

type Andes struct {
	headwaters *Headwaters
	first      Stream
	last       Stream
}

func (an *Andes) Start() (id string, err error) {
	if an.first == nil {
		return "", errors.New("no river can run")
	}
	if an.headwaters == nil {
		return "", errors.New("no headwaters can't run")
	}

	if an.headwaters.atlantic == nil {
		return "", errors.New("no atlantic can't run")
	}
	rootId, err := saveDraw(an, stateInit)
	if err != nil {
		return "", err
	}
	go an.first.Run(an.headwaters, an.first.(RiverFlow), false)
	return rootId, nil
}

func (an *Andes) ReStart() error {
	if an.first == nil {
		return errors.New("no river can run")
	}
	if an.headwaters == nil {
		return errors.New("no headwaters can't run")
	}

	if an.headwaters.atlantic == nil {
		return errors.New("no atlantic can't run")
	}
	go an.first.Run(an.headwaters, an.first.(RiverFlow), false)
	return nil
}

func (an *Andes) GetHeadwaters() *Headwaters {
	return an.headwaters
}

func (an *Andes) DrawStream(river Stream) *Andes {
	if _, ok := river.(RiverFlow); !ok {
		panic("river not implement RiverFlow")
	}
	if an.first == nil {
		an.first = river
		an.last = river
	} else {
		an.last.SetDownStream(river)
		an.last = river
	}
	for nxt := river.next(); nxt != nil; nxt = nxt.next() {
		an.last.SetDownStream(nxt)
		an.last = nxt
	}

	return an
}

func (an *Andes) DrawHeadWaters(waters *Headwaters) *Andes {
	an.headwaters = waters
	return an
}

func (an *Andes) DrawAtlantic(atlantic AtlanticStream) *Andes {
	if an.headwaters == nil {
		panic("headwaters not set")
	}
	if _, ok := atlantic.(AtlanticFlow); !ok {
		panic("atlantic not implement AtlanticFlow")
	}
	an.headwaters.atlantic = atlantic
	return an
}

func (an *Andes) outInner(rivers []Stream, startIndex int) {
	for _, v := range rivers {
		switch v.(type) {
		case *ParallelRiver:
			vf := &VastFlow{
				Id:       v.getId(),
				Index:    startIndex,
				FlowType: flowTypeParallel,
				Name:     reflect.TypeOf(v).Elem().Name(),
				Color:    v.GetColor(),
				State:    v.getState().String(),
				WaterId:  v.getWaterId(),
			}
			logs.Debug("%v", vf)
			vv, ok := v.(ParallelStream)
			if !ok {
				logs.Error("invalid parallel river")
				return
			}
			for _, r := range vv.getRivers() {
				rivers := make([]Stream, 0)
				rivers = append(rivers, r)
				an.outInner(rivers, 0)
			}

		case *RiverBasin:
			vf := &VastFlow{
				Id:       v.getId(),
				Index:    startIndex,
				FlowType: flowTypeBasin,
				Name:     reflect.TypeOf(v).Elem().Name(),
				Color:    v.GetColor(),
				State:    v.getState().String(),
				WaterId:  v.getWaterId(),
			}
			logs.Debug("%v", vf)
			vv, ok := v.(BasinStream)
			if !ok {
				logs.Error("invalid basin type")
				return
			}
			rivers := make([]Stream, 0)
			rivers = append(rivers, vv.getFirstDream())
			an.outInner(rivers, 0)

		default:
			vf := &VastFlow{
				Id:       v.getId(),
				Index:    startIndex,
				FlowType: flowTypeCommon,
				Name:     reflect.TypeOf(v).Elem().Name(),
				Color:    v.GetColor(),
				State:    v.getState().String(),
				WaterId:  v.getWaterId(),
			}
			logs.Debug("%v", vf)
		}

		if nxt := v.next(); nxt != nil {
			nextRivers := make([]Stream, 0)
			nextRivers = append(nextRivers, nxt)
			an.outInner(nextRivers, startIndex+1)
		}
	}
}
func (an *Andes) Out() {
	if an.headwaters == nil {
		logs.Debug("headwaters not set")
		return
	}
	logs.Debug("headwaters:%s", toPersistWater(an.headwaters))
	at := an.headwaters.atlantic
	vf := &VastFlow{
		Id:       at.getId(),
		Index:    -1,
		FlowType: flowTypeAtlantic,
		Name:     reflect.TypeOf(at).Elem().Name(),
		Color:    "",
		State:    at.getState().String(),
		WaterId:  at.getWaterId(),
	}
	logs.Debug("atlantic:%v", vf)
	rivers := make([]Stream, 0)
	rivers = append(rivers, an.first)
	an.outInner(rivers, 0)
}
