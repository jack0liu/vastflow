package vastflow

type Stream interface {
	Run(waters *Headwaters, flow RiverFlow, syncNext bool) error
	SetDownStream(down Stream)  // set next stream
	next() Stream
	setInner()     // if parallel inner river

	setId(id string)
	getId() string
	setColor(color string)
	colorCurrent(color string) // used for river load
	GetColor() string
	setState(state streamState)
	getState() streamState
	setWaterId(waterId string)
	getWaterId() string
}

type streamState int32

func (s streamState) String() string {
	switch s {
	case stateInit:
		return "init"
	case stateRunning:
		return "running"
	case stateCycling:
		return "cycling"
	case stateSuccess:
		return "success"
	case stateFail:
		return "fail"
	default:
		return "unknown"
	}
}

const (
	stateInit    streamState = 0
	stateRunning streamState = 1
	stateCycling streamState = 2
	stateSuccess streamState = 3
	stateFail    streamState = 4
)

var stateMap map[string]streamState

func init() {
	stateMap = make(map[string]streamState, 5)
	stateMap["init"] = stateInit
	stateMap["running"] = stateRunning
	stateMap["cycling"] = stateCycling
	stateMap["success"] = stateSuccess
	stateMap["fail"] = stateFail
}
