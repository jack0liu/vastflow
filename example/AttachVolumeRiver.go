package main

import (
	"fmt"
	"github.com/jack0liu/vastflow"
)

type AttachVolumeRiver struct {
	vastflow.River
}

func init() {
	vastflow.RegisterStream(new(AttachVolumeRiver))
}

func (r *AttachVolumeRiver) Update(attr *vastflow.RiverAttr) {
	attr.CycleInterval = 1
	attr.CycleTimes = 3
	attr.Durable = true
}

func (r *AttachVolumeRiver) Flow(headwaters *vastflow.Headwaters) (errCause string, err error) {
	fmt.Println(fmt.Sprintf("AttachVolumeRiver flow, vmId:%s, volumeId:%s", headwaters.GetString("vmId"), headwaters.GetString("volId")))
	fmt.Println("attach volume success")
	return "", nil
}

func (r *AttachVolumeRiver) Cycle(headwaters *vastflow.Headwaters) (errCause string, err error) {
	return "", nil
}