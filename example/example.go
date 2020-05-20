package main

import (
	"github.com/jack0liu/vastflow"
	uuid "github.com/satori/go.uuid"
	"time"
)

func main() {
	// headerwaters set
	vastflow.InitVastFlowDb("vastflow.json", "")
	hw := vastflow.NewHeadwaters(uuid.NewV4().String())
	hw.Put("vmName", "my-vm")
	hw.Put("volName", "my-vol")

	// parallel river draw
	pr := &vastflow.ParallelRiver{}
	pr.Append(new(CreateVmRiver))
	pr.Append(new(CreateVolumeRiver))

	// andes draw
	an := vastflow.Andes{}
	an.DrawHeadWaters(hw)
	an.DrawStream(pr)
	an.DrawStream(new(AttachVolumeRiver))
	// atlantic draw
	an.DrawAtlantic(new(AttachVolAtlantic))

	an.Start()

	time.Sleep(time.Second * 10)
}