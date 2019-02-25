// Package timewheel implement a time wheel similar with Netty's HashedTimeWheel
package timewheel

import (
	"container/list"
	"errors"
	"fmt"
	"github.com/FelixSeptem/concurrent-map"
	"github.com/cespare/xxhash"
	"github.com/denisbrodbeck/machineid"
	"github.com/sony/sonyflake"
	"strconv"
	"sync"
	"time"
)

// some default config parameters
const (
	DEFAULT_TIMEWHEEL_SLOTSNUM     = 3600
	DEFAULT_TIMEWHEEL_STEPDURATION = time.Microsecond
	DEFAULT_TIMEWHEEL_ERRORSIZE    = 1024
)

// timewheel running status
const (
	TIMEWHEEL_RUNNING_STATUS_INIT = iota
	TIMEWHEEL_RUNNING_STATUS_RUNNING
	TIMEWHEEL_RUNNING_STATUS_END
)

// task handler function's signature
type TaskHandler func() error

type myStr string

func (s myStr) String() string {
	return string(s)
}

type taskID struct {
	id       string
	cycleNum int
}

// timewheel entity
type TimeWheel struct {
	capacityLock  sync.RWMutex
	name          string
	startTime     time.Time
	idGen         *sonyflake.Sonyflake
	timewheel     []*taskList
	pivot         int
	slotsNum      int
	stepDuration  time.Duration
	taskData      *cmap.ConcurrentMap
	errs          chan error
	cycleTime     time.Duration
	capacity      int64
	quit          chan struct{}
	runningStatus int
}

type taskList struct {
	mutex sync.RWMutex
	tasks *list.List
}

// return a new time wheel of given parameters for example:
// NewTimeWheel("example", 3600, time.Second, 100)
// will return a TimeWheel which has accuracy of 1 second and running around once an hour(3600 * 1s), it has an err channel to receive
// task handler's return err,which has 100 capacity.
func NewTimeWheel(name string, slotsNum int, stepDuration time.Duration, errSize int) *TimeWheel {
	if slotsNum <= 0 {
		slotsNum = DEFAULT_TIMEWHEEL_SLOTSNUM
	}
	if stepDuration <= DEFAULT_TIMEWHEEL_STEPDURATION {
		stepDuration = DEFAULT_TIMEWHEEL_STEPDURATION
	}
	if errSize <= 0 {
		errSize = DEFAULT_TIMEWHEEL_ERRORSIZE
	}
	data := cmap.New(slotsNum)
	tw := make([]*taskList, slotsNum)
	for i := range tw {
		tw[i] = &taskList{
			tasks: list.New(),
		}
	}
	st := time.Now()
	return &TimeWheel{
		name:      name,
		startTime: st,
		idGen: sonyflake.NewSonyflake(sonyflake.Settings{
			StartTime: st,
			MachineID: func() (uint16, error) {
				return getMachineID()
			},
			CheckMachineID: func(u uint16) bool {
				mid, err := getMachineID()
				if err != nil {
					return false
				}
				return mid == u
			},
		}),
		timewheel:    tw,
		slotsNum:     slotsNum,
		stepDuration: stepDuration,
		taskData:     data,
		errs:         make(chan error, errSize),
		cycleTime:    stepDuration * time.Duration(slotsNum),
		quit:         make(chan struct{}, slotsNum),
	}
}

func (tw *TimeWheel) getUID() (string, error) {
	id, err := tw.idGen.NextID()
	if err != nil {
		return "", err
	}
	return strconv.FormatUint(id, 16), nil
}

func getMachineID() (uint16, error) {
	mId, err := machineid.ID()
	if err != nil {
		return 0, err
	}
	return uint16(xxhash.Sum64String(mId) % 65535), nil
}

// add a new task into time wheel return task's ID
func (tw *TimeWheel) AddTask(delayDurations time.Duration, handler TaskHandler) (string, error) {
	if delayDurations <= 0 {
		return "", fmt.Errorf("invalid time:%v", delayDurations)
	}
	slotLocation := int((delayDurations % tw.cycleTime) / tw.stepDuration)
	cycleNum := delayDurations / tw.cycleTime
	id, err := tw.getUID()
	if err != nil {
		return "", err
	}
	tw.addTaskTotimewheel(tw.timewheel[slotLocation], &taskID{
		id:       id,
		cycleNum: int(cycleNum),
	})
	tw.taskData.Set(myStr(id), handler)
	tw.capacityLock.Lock()
	tw.capacity += 1
	defer tw.capacityLock.Unlock()
	return id, nil
}

// start time consume
func (tw *TimeWheel) Run() error {
	if tw.runningStatus != TIMEWHEEL_RUNNING_STATUS_INIT {
		return errors.New("invalid timewheel running status")
	}
	go func() {
		tw.runningStatus = TIMEWHEEL_RUNNING_STATUS_RUNNING
		ticker := time.NewTicker(tw.stepDuration)
		pivot := tw.pivot
		for {
			select {
			case <-ticker.C:
				if pivot >= tw.slotsNum {
					pivot = pivot % tw.slotsNum
				}
				tw.pivot = pivot
				tw.processHandler(tw.timewheel[pivot])
				pivot += 1
			case <-tw.quit:
				tw.runningStatus = TIMEWHEEL_RUNNING_STATUS_END
				return
			}
		}
	}()
	return nil
}

// return time wheel's related information
func (tw *TimeWheel) Info() (name string, startTime time.Time, capacity int64) {
	tw.capacityLock.RLock()
	defer tw.capacityLock.RUnlock()
	return tw.name, tw.startTime, tw.capacity
}

// return task handler's errs to handle
func (tw *TimeWheel) HandleErr() <-chan error {
	return tw.errs
}

// quit timewheel may lost unfinished task, concurrent quit may cause panic due to golang's channel close principle
func (tw *TimeWheel) Quit() error {
	if tw.runningStatus != TIMEWHEEL_RUNNING_STATUS_RUNNING {
		return fmt.Errorf("invalid running status:%d", tw.runningStatus)
	}
	close(tw.quit)
	return nil
}

// BQuit will blocking until all task in the timewheel has been finished,then quit
func (tw *TimeWheel) BQuit() error {
	if tw.runningStatus != TIMEWHEEL_RUNNING_STATUS_RUNNING {
		return fmt.Errorf("invalid running status:%d", tw.runningStatus)
	}
	go func() {
		for {
			tw.capacityLock.RLock()
			if tw.capacity == 0 && tw.runningStatus == TIMEWHEEL_RUNNING_STATUS_RUNNING {
				close(tw.quit)
				return
			}
			tw.capacityLock.RUnlock()

		}
	}()
	return nil
}

func (tw *TimeWheel) processHandler(tl *taskList) {
	tl.mutex.Lock()
	defer tl.mutex.Unlock()
	if tl.tasks.Len() == 0 {
		return
	}
	for v := tl.tasks.Front(); v != nil; v = v.Next() {
		n := v
		if t := n.Value.(*taskID); t.cycleNum == 0 {
			go func() {
				defer func() {
					tw.capacityLock.Lock()
					tw.capacity -= 1
					tw.capacityLock.Unlock()
					tl.mutex.Lock()
					tl.tasks.Remove(n)
					tl.mutex.Unlock()
				}()
				fun, ok := tw.taskData.Get(myStr(t.id))
				if !ok {
					tw.errs <- fmt.Errorf("%s not found", t.id)
					return
				}
				err := fun.(TaskHandler)()
				if err != nil {
					tw.errs <- fmt.Errorf("id:%s with %v", t.id, err)
				}
			}()
		} else {
			t.cycleNum -= 1
		}
	}
}

func (tw *TimeWheel) addTaskTotimewheel(tl *taskList, task *taskID) {
	tl.mutex.Lock()
	defer tl.mutex.Unlock()
	tl.tasks.PushBack(task)
}
