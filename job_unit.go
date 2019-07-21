package vastflow

import (
	"github.com/astaxie/beego/orm"
	"github.com/jack0liu/logs"
	"time"
)

const (
	UnitActive = "active"
	UnitDead   = "dead"
)

type JobUnit struct {
	Name      string    `orm:"size(64);pk"`
	Status    string    `orm:"size(64)"`
	CreateAt  time.Time `orm:"null;type(datetime);column(create_at)"`
	UpdatedAt time.Time `orm:"null;type(datetime);column(updated_at)"`
}

func init() {
	// register model
	orm.RegisterModel(new(JobUnit))
}

func SaveUnit(c JobUnit) error {
	o := orm.NewOrm()
	_, err := o.Insert(&c)
	if err != nil {
		logs.Error("insert error:%s", err.Error())
		return err
	}

	return nil
}

func getUnit(unit string) (jobUnit *JobUnit, err error) {
	o := orm.NewOrm()
	var ju JobUnit
	if err := o.QueryTable("job_unit").Filter("name", unit).One(&ju); err != nil {
		return nil, err
	}
	return &ju, nil
}

func touchUnit(unit string) error {
	o := orm.NewOrm()
	job := JobUnit{
		Name:      unit,
		UpdatedAt: time.Now().UTC(),
		Status:    UnitActive,
	}
	if _, err := o.Update(&job, "updated_at", "status"); err != nil {
		logs.Error("update fail,%s", err.Error())
		return err
	}
	return nil
}

func UpdateUnitStatus(name, status string, o orm.Ormer) error {
	if o == nil {
		o = orm.NewOrm()
	}
	job := JobUnit{
		Name:   name,
		Status: status,
	}
	if _, err := o.Update(&job, "status"); err != nil {
		logs.Error("update fail,%s", err.Error())
		return err
	}
	return nil
}

func getOtherUnit(myUnit string) []*JobUnit {
	o := orm.NewOrm()
	var units []*JobUnit
	_, err := o.QueryTable("job_unit").
		Filter("status", UnitActive).
		Exclude("name", myUnit).All(&units)
	if err != nil {
		logs.Error("getOtherUnit fail,%s", err.Error())
		return nil
	}
	return units
}
