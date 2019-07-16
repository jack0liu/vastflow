package vastflow

import (
	"fmt"
        "github.com/jack0liu/logs"
	"reflect"
)

var streamTypes = make(map[string] reflect.Type)

var atlanticTypes = make(map[string] reflect.Type)

func RegisterStream(stream Stream) {
	if stream == nil {
		panic("vastflow:Register stream is nil")
	}
	streamName := reflect.TypeOf(stream).Elem().Name()
	logs.Debug("register stream: %s", streamName)
	if _, ok := streamTypes[streamName]; ok {
		panic(fmt.Sprintf("vastflow:RegisterStream is dumplicated, name:%q", streamName))
	}
	streamTypes[streamName] = reflect.TypeOf(stream).Elem()
}

func RegisterAtlantic(atlantic AtlanticStream) {
	if atlantic == nil {
		panic("vastflow:Register atlantic is nil")
	}
	atlanticName := reflect.TypeOf(atlantic).Elem().Name()
	logs.Debug("register atlantic:%s", atlanticName)
	if _, ok := atlanticTypes[atlanticName]; ok {
		panic(fmt.Sprintf("vastflow:RegisterAtlantic is dumplicated, name:%q", atlanticName))
	}
	atlanticTypes[atlanticName] = reflect.TypeOf(atlantic).Elem()
}

func getStream(name string) reflect.Type {
	if st, ok := streamTypes[name]; ok {
		return st
	} else {
		logs.Error("not found stream :%s", name)
		return nil
	}
}

func getAtlantic(name string) reflect.Type {
	if at, ok := atlanticTypes[name]; ok {
		return at
	} else {
		logs.Error("not found atlantic :%s", name)
		return nil
	}
}
