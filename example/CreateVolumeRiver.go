package main

import (
	"fmt"
	"github.com/jack0liu/vastflow"
	"time"
)

type CreateVolumeRiver struct {
	vastflow.River
}

func init() {
	vastflow.RegisterStream(new(CreateVolumeRiver))
}

func (r *CreateVolumeRiver) Update(attr *vastflow.RiverAttr) {
	attr.CycleInterval = 1
	attr.CycleTimes = 3
	attr.Durable = true
}

func (r *CreateVolumeRiver) Flow(headwaters *vastflow.Headwaters) (errCause string, err error) {
	fmt.Println("CreateVolumeRiver flow, name:", headwaters.Get("volName"))
	time.Sleep(time.Second)
	volumeId := "volId1"
	headwaters.Put("volId", volumeId)
	fmt.Println("create volume req over")
	return "", nil
}

func (r *CreateVolumeRiver) Cycle(headwaters *vastflow.Headwaters) (errCause string, err error) {
	fmt.Println("CreateVolumeRiver cycle, volId:", headwaters.Get("volId"))
	fmt.Println("create volume success")
	return "", nil
}