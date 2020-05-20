package main

import (
	"fmt"
	"github.com/jack0liu/vastflow"
)

type AttachVolAtlantic struct {
	vastflow.Atlantic
}

func init() {
	vastflow.RegisterAtlantic(new(AttachVolAtlantic))
}

func (r *AttachVolAtlantic) Success(headwaters *vastflow.Headwaters) error {
	fmt.Println("attach success, we should update status")
	return nil
}

func (r *AttachVolAtlantic) Fail(headwaters *vastflow.Headwaters) error {
	fmt.Println("attach fail, we should send alarm")
	return nil
}
