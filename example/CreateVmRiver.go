package main

import (
	"fmt"
	"github.com/jack0liu/vastflow"
	"time"
)

type CreateVmRiver struct {
	vastflow.River
}

func init() {
	vastflow.RegisterStream(new(CreateVmRiver))
}

func (r *CreateVmRiver) Update(attr *vastflow.RiverAttr) {
	attr.CycleInterval = 1
	attr.CycleTimes = 10
	attr.Durable = true
}

func (r *CreateVmRiver) Flow(headwaters *vastflow.Headwaters) (errCause string, err error) {
	fmt.Println("CreateVmRiver flow, name:", headwaters.Get("vmName"))
	time.Sleep(time.Second)
	vmId := "vmId1"
	headwaters.Put("vmId", vmId)
	fmt.Println("create vm req over")
	return "", nil
}

func (r *CreateVmRiver) Cycle(headwaters *vastflow.Headwaters) (errCause string, err error) {
	fmt.Println("CreateVmRiver cycle, vmId:", headwaters.Get("vmId"))
	fmt.Println("create vm success")
	return "", nil
}