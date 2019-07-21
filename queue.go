package vastflow

import (
	"errors"
	"fmt"
	"github.com/astaxie/beego/orm"
	"github.com/jack0liu/logs"
	"github.com/satori/go.uuid"
	"strings"
	"time"
)

const (
	JobWaiting = "waiting"
	JobRunning = "running"
	JobFailed  = "failed"
	JobSuccess = "success"

	maxContinueCount = 10
)

var (
	slowExpandCount = 0
)

type JobQueue struct {
	Id        string    `orm:"size(64);column(id);pk"`
	RequestId string    `orm:"size(64)"`
	Status    string    `orm:"size(64)"`
	Action    string    `orm:"size(64)"`
	ProjectId string    `orm:"size(64)"`
	ObjectId  string    `orm:"null;size(64)"`
	ProcUnit  string    `orm:"null;size(64)"`
	CreateAt  time.Time `orm:"null;type(datetime);column(create_at)"`
	UpdatedAt time.Time `orm:"null;type(datetime);column(updated_at)"`
	EncToken  string    `orm:"null;type(text)"`
	Request   string    `orm:"null;type(text)"`
}

func (sys *JobQueue) TableName() string {
	return "job_queue"
}

func init() {
	// register model
	orm.RegisterModel(new(JobQueue))
}

func SaveJob(c JobQueue, o orm.Ormer) (id string, err error) {
	if o == nil {
		logs.Info("o is nil")
		o = orm.NewOrm()
	}
	c.Id = strings.Replace(uuid.NewV4().String(), "-", "", -1)
	c.CreateAt = time.Now().UTC()
	c.UpdatedAt = time.Now().UTC()
	_, err = o.Insert(&c)
	if err != nil {
		logs.Error("insert error:%s", err.Error())
		return "", err
	}

	return c.Id, nil
}

func GetJobByRequestId(requestId string) (job *JobQueue, err error) {
	var jqs []*JobQueue
	o := orm.NewOrm()
	_, err = o.QueryTable("job_queue").
		Filter("request_id", requestId).
		All(&jqs)
	if err != nil {
		logs.Error("query job by request id(%s) fail", requestId)
		return nil, err
	}
	if len(jqs) == 0 {
		logs.Error("no job by request id(%s)", requestId)
		return nil, nil
	}
	if len(jqs) > 1 {
		logs.Error("multi jobs by request id(%s)", requestId)
		return nil, errors.New("multi jobs error")
	}
	return jqs[0], err
}

func GetOneWaitingJob(unit string) (job *JobQueue, err error) {
	job, err = fetchWaitingJobByUnit(unit)
	if job != nil {
		return
	}
	if err == orm.ErrNoRows {
		// no row found
		job, err = fetchWaitingJobByUnit("")
		if job == nil {
			return nil, err
		}
		o := orm.NewOrm()
		num, err := o.QueryTable("job_queue").
			Filter("id", job.Id).
			Filter("proc_unit", ""). // avoid other update this
			Update(orm.Params{
				"proc_unit":  unit,
				"updated_at": time.Now().UTC(),
			})
		if err != nil {
			logs.Info("can't update unit, err:%s", err.Error())
			return nil, err
		}
		if num == 0 {
			logs.Info("update proc unit nothing, not get waiting job")
			return nil, errors.New("not get waiting job, next")
		}
		job.ProcUnit = unit
		return job, nil
	}
	return nil, err
}

func fetchWaitingJobByUnit(unit string) (job *JobQueue, err error) {
	var j JobQueue
	o := orm.NewOrm()
	err = o.QueryTable("job_queue").
		Filter("status", JobWaiting).
		Filter("proc_unit", unit).
		OrderBy("updated_at").
		Limit(1).
		One(&j)
	if err != nil {
		return nil, err
	}
	flowWnd.Lock()
	defer flowWnd.Unlock()
	if flowWnd.Remains() <= 0 {
		errStr := fmt.Sprintf("not have enough wnd, curSize:%d, total capacity:%d",
			flowWnd.CurSize, flowWnd.Capacity+flowWnd.extended)
		logs.Info(errStr)
		if flowWnd.IsBelowMark() {
			slowExpandCount++
			if slowExpandCount >= maxContinueCount {
				flowWnd.Extend(defaultExtendLen)
			}
		} else {
			logs.Info("over mark, clean count")
			slowExpandCount = 0
		}
		return nil, errors.New(errStr)
	}
	slowExpandCount = 0
	num, err := o.QueryTable("job_queue").
		Filter("id", j.Id).
		Filter("status", JobWaiting). // avoid other update this
		Update(orm.Params{
			"status":     JobRunning,
			"updated_at": time.Now().UTC(),
		})
	if err != nil {
		logs.Info("can't update status, err:%s", err.Error())
		return nil, err
	}
	if num == 0 {
		logs.Info("update job nothing, not get waiting job, next")
		return nil, nil
	}
	flowWnd.Inc()
	return &j, nil
}

func SetRunningJobFailed(jobId string) error {
	o := orm.NewOrm()
	num, err := o.QueryTable("job_queue").
		Filter("id", jobId).
		Filter("status", JobRunning). // avoid other update this
		Update(orm.Params{
			"status":     JobFailed,
			"updated_at": time.Now().UTC(),
		})
	if err != nil {
		logs.Info("can't update status, err:%s", err.Error())
		return err
	}
	if num == 0 {
		logs.Info("update running job nothing.")
		return nil
	}
	flowWnd.Lock()
	flowWnd.Dec()
	flowWnd.Unlock()
	return nil
}

func UpdateJobStatus(jobId, status string) error {
	o := orm.NewOrm()
	job := JobQueue{
		Id:        jobId,
		Status:    status,
		UpdatedAt: time.Now().UTC(),
	}
	if _, err := o.Update(&job, "status", "updated_at"); err != nil {
		logs.Error("update job fail,%s", err.Error())
		return err
	}
	return nil
}

func UnSetJobUnit(unit string, o orm.Ormer) error {
	if o == nil {
		o = orm.NewOrm()
	}
	num, err := o.QueryTable("job_queue").
		Filter("status", JobRunning).
		Filter("proc_unit", unit).
		Update(orm.Params{
			"proc_unit": "",
			"status":    JobWaiting,
		})
	if err != nil {
		logs.Error("update fail err:%s", err.Error())
		return err
	}
	logs.Info("unset job unit(%s) num %d", unit, num)
	return nil
}

func transJobStatusByUnit(unit, fromStatus, toStatus string) error {
	o := orm.NewOrm()
	num, err := o.QueryTable("job_queue").
		Filter("proc_unit", unit).
		Filter("status", fromStatus). // avoid other update this
		Update(orm.Params{
			"status":     toStatus,
			"updated_at": time.Now().UTC(),
		})
	if err != nil {
		logs.Info("can't update status, err:%s", err.Error())
		return err
	}
	if num == 0 {
		logs.Info("not update any job")
		return nil
	}
	logs.Debug("trans job num:%d", num)
	return nil
}
