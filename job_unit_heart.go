package vastflow

import (
	"github.com/jack0liu/logs"
	"errors"
	"github.com/astaxie/beego/orm"
	"sync"
	"time"
)

const (
	defaultHeartInterval = 2
	displayTimes = 5
)

var (
	heartOnce = sync.Once{}
	checkOnce = sync.Once{}
	shrinkOnce = sync.Once{}
	thisUnit string
	intervalSec int
)

func StartHeart(myUnit string, heartIntervalSec int) error {
	if len(myUnit) == 0 {
		return errors.New("my unit is empty")
	}
	thisUnit = myUnit
	intervalSec = defaultHeartInterval
	if heartIntervalSec > 0 {
		intervalSec = heartIntervalSec
	}

	// check unit is valid
	unit, err := getUnit(myUnit)
	if err == orm.ErrNoRows {
		newUnit := JobUnit{
			Name: myUnit,
			Status: UnitActive,
			CreateAt: time.Now().UTC(),
			UpdatedAt: time.Now().UTC(),
		}
		if err := SaveUnit(newUnit); err != nil {
			return err
		}
		unit = &newUnit
	} else if err != nil {
		logs.Error("get unit fail")
		return err
	} else {
		now := time.Now().UTC()
		if unit.UpdatedAt.Add(2 * time.Duration(intervalSec) * time.Second).After(now) {
			logs.Error("replicate unit(%s)", myUnit)
			return errors.New("replicate unit")
		}
		// check if exist job is running
		_ = transJobStatusByUnit(myUnit, JobRunning, JobWaiting)
	}

	if err := touchUnit(myUnit); err != nil {
		logs.Error("keep heart fail")
		return err
	}

	go heartOnce.Do(heart)
	go checkOnce.Do(checkOtherIfDead)
	go shrinkOnce.Do(checkShrink)
	return nil
}


func heart() {
	t := time.NewTicker(time.Duration(intervalSec) * time.Second)
	displayIntervalCount := 0
	for {
		select {
		case <-t.C:
			if err := touchUnit(thisUnit); err != nil {
				logs.Error("keep heart fail")
			}
			displayIntervalCount++
			if displayIntervalCount == displayTimes {
				displayIntervalCount = 0
				flowWnd.Print()
			}
		}
	}
}

func checkOtherIfDead() {
	checkInterval := 5 * intervalSec
	t := time.NewTicker(time.Duration(checkInterval) * time.Second)
	for {
		select {
		case <-t.C:
			units := getOtherUnit(thisUnit)
			now := time.Now().UTC()
			for _, u := range units {
				if now.After(u.UpdatedAt.Add(3 * time.Duration(checkInterval) * time.Second)) {
					logs.Info("unit(%s) dead", u.Name)
					o := orm.NewOrm()
					if err := o.Begin(); err != nil {
						logs.Warn("begin fail")
						continue
					}
					if err := UpdateUnitStatus(u.Name, UnitDead, o); err != nil {
						logs.Error("update unit(%s) dead fail, err:%s", u.Name, err.Error())
						_ = o.Rollback()
						continue
					}
					if err := UnSetJobUnit(u.Name, o); err != nil {
						logs.Error("unset unit(%s) fail, err:%s", u.Name, err.Error())
						_ = o.Rollback()
						continue
					}
					if err := o.Commit(); err != nil {
						logs.Error("commit err:%s", err.Error())
					}
					logs.Debug("update unit(%s) dead success", u.Name)
				}
			}
		}
	}
}
