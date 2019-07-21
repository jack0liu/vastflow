package vastflow

import (
	"errors"
	"github.com/jack0liu/logs"
	"github.com/satori/go.uuid"
	"reflect"
	"sync"
)

type errorBasin struct {
	err error
}

type Headwaters struct {
	// global context
	RequestId string
	ReqInfo   interface{} // request info from andes
	atlantic  AtlanticStream

	// basin context
	basinMu   *sync.Mutex
	basinDone chan struct{} // must comes from basin
	basinErr  *errorBasin

	// river context
	id      string
	mu      sync.RWMutex           // protects following fields
	context map[string]interface{} // water's context
	done    chan struct{}
	err     error // set to non-nil by the first cancel call

	// tmp context , not persist
	tmpMu      sync.RWMutex
	tmpContext map[string]interface{}
}

// closedChan is a reusable closed channel.
var closedChan = make(chan struct{})

func init() {
	close(closedChan)
}

func NewHeadwaters(requestId string) *Headwaters {
	if len(requestId) == 0 {
		logs.Error("new headwaters fail, requestId is empty")
		return nil
	}
	return &Headwaters{
		RequestId: requestId,
		basinMu:   &sync.Mutex{},
		basinDone: make(chan struct{}),
		basinErr:  &errorBasin{},

		context:    make(map[string]interface{}, 0),
		tmpContext: make(map[string]interface{}, 0),
		done:       make(chan struct{}),
	}
}

func (hw *Headwaters) Put(key string, val interface{}) {
	hw.mu.Lock()
	defer hw.mu.Unlock()
	if hw.context == nil {
		hw.context = make(map[string]interface{}, 0)
	}
	hw.context[key] = val
}

func (hw *Headwaters) Del(key string) {
	hw.mu.Lock()
	defer hw.mu.Unlock()
	if hw.context == nil {
		return
	}
	delete(hw.context, key)
}

func (hw *Headwaters) Replace(other *Headwaters) {
	hw.mu.Lock()
	defer hw.mu.Unlock()
	hw.context = other.context
}

func (hw *Headwaters) Get(key string) interface{} {
	hw.mu.RLock()
	defer hw.mu.RUnlock()
	if hw.context == nil {
		logs.Error("context is nil")
		return nil
	}
	return hw.context[key]
}

func (hw *Headwaters) GetInt(key string) int {
	hw.mu.RLock()
	defer hw.mu.RUnlock()
	if hw.context == nil {
		logs.Error("context is nil")
		return -1
	}
	v := hw.context[key]
	switch v.(type) {
	case int:
		return v.(int)
	case float64:
		return int(v.(float64))
	default:
		logs.Warn("[%s]'s value[%v] is not int, is %v", key, v, reflect.TypeOf(v))
	}
	return -1
}

func (hw *Headwaters) GetString(key string) string {
	hw.mu.RLock()
	defer hw.mu.RUnlock()
	if hw.context == nil {
		logs.Error("context is nil")
		return ""
	}
	v := hw.context[key]
	if v == nil {
		return ""
	}
	if vs, ok := v.(string); ok {
		return vs
	}
	logs.Debug("[%s]'s value[%v] is not string, is %v", key, v, reflect.TypeOf(v))
	return ""
}

func (hw *Headwaters) GetAll() map[string]interface{} {
	hw.mu.RLock()
	defer hw.mu.RUnlock()
	if hw.context == nil {
		return nil
	}
	return hw.context
}

func (hw *Headwaters) Cancel(err error) {
	hw.mu.Lock()
	defer hw.mu.Unlock()
	if hw.err != nil {
		return // already canceled
	}
	hw.err = err
	if hw.done == nil {
		hw.done = closedChan
	} else {
		close(hw.done)
	}
}

func (hw *Headwaters) Done() <-chan struct{} {
	hw.mu.RLock()
	defer hw.mu.RUnlock()
	if hw.err != nil {
		hw.done = closedChan
	}
	if hw.done == nil {
		hw.done = make(chan struct{})
	}
	d := hw.done
	return d
}

func (hw *Headwaters) Err() error {
	hw.mu.RLock()
	defer hw.mu.RUnlock()
	err := hw.err
	return err
}

func (hw *Headwaters) copy4Basin() *Headwaters {
	newContext := make(map[string]interface{}, 0)
	newTmpContext := make(map[string]interface{}, 0)
	hw.mu.RLock()
	defer hw.mu.RUnlock()
	logs.Debug("copy4Basin")

	for k, v := range hw.context {
		newContext[k] = v
	}

	for k, v := range hw.tmpContext {
		newTmpContext[k] = v
	}

	newHeadwaters := &Headwaters{
		RequestId: hw.RequestId,
		ReqInfo:   hw.ReqInfo,
		atlantic:  hw.atlantic,

		basinMu:   hw.basinMu,
		basinDone: hw.basinDone,
		basinErr:  hw.basinErr,

		// something new
		id:      uuid.NewV4().String(),
		context: newContext,
		done:    make(chan struct{}),

		tmpContext: newTmpContext,
	}
	return newHeadwaters
}

func (hw *Headwaters) TmpPut(key string, value string) {
	hw.tmpMu.Lock()
	defer hw.tmpMu.Unlock()
	if hw.tmpContext == nil {
		hw.tmpContext = make(map[string]interface{}, 0)
	}
	hw.tmpContext[key] = value
}

func (hw *Headwaters) TmpGet(key string) string {
	hw.tmpMu.RLock()
	defer hw.tmpMu.RUnlock()
	if hw.tmpContext == nil {
		logs.Info("tmpContext is nil")
		return ""
	}
	v := hw.tmpContext[key]
	if vs, ok := v.(string); ok {
		return vs
	}
	logs.Debug("[%s]'s value[%v] is not string", key, v)
	return ""
}

func (hw *Headwaters) basinFinish() <-chan struct{} {
	hw.basinMu.Lock()
	defer hw.basinMu.Unlock()
	if hw.basinDone == nil {
		logs.Info("basinDone is nil")
		hw.basinDone = closedChan
	}
	d := hw.basinDone
	return d
}

func (hw *Headwaters) basinCancel(err error) {
	hw.basinMu.Lock()
	defer hw.basinMu.Unlock()
	if hw.basinErr == nil {
		logs.Error("basinErr is nil") // headwaters is created incorrectly
		return
	}
	if hw.basinErr.err != nil {
		return // already canceled
	}
	hw.basinErr.err = err
	if hw.basinDone == nil {
		hw.basinDone = closedChan
	} else {
		close(hw.basinDone)
	}
}

func (hw *Headwaters) basinError() error {
	hw.basinMu.Lock()
	defer hw.basinMu.Unlock()
	if hw.basinErr == nil {
		logs.Error("basinErr is nil") // headwaters is created incorrectly
		return errors.New("headwaters is created incorrectly, basinErr is nil")
	}
	err := hw.basinErr.err
	return err
}
