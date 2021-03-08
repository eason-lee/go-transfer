package executors

import (
	"go-transfer/service"
	"go-transfer/utils"
	"reflect"
	"sync"
	"time"
)

const idleRound = 10

type (
	// A type that satisfies executors.TaskContainer can be used as the underlying
	// container that used to do periodical executions.
	TaskContainer interface {
		// AddTask adds the task into the container.
		// Returns true if the container needs to be flushed after the addition.
		AddTask(task interface{}) bool
		// Execute handles the collected tasks by the container when flushing.
		Execute(tasks interface{})
		// RemoveAll removes the contained tasks, and return them.
		RemoveAll() interface{}
	}

	Barrier struct {
		lock sync.Mutex
	}

	PeriodicalExecutor struct {
		commander chan interface{}
		interval  time.Duration
		container TaskContainer
		waitGroup sync.WaitGroup
		// avoid race condition on waitGroup when calling wg.Add/Done/Wait(...)
		wgBarrier   Barrier
		confirmChan chan utils.PlaceholderType
		guarded     bool
		newTicker   func(duration time.Duration) *time.Ticker
		lock        sync.Mutex
	}
)

func (b *Barrier) Guard(fn func()) {
	b.lock.Lock()
	defer b.lock.Unlock()
	fn()
}

func NewPeriodicalExecutor(interval time.Duration, container TaskContainer) *PeriodicalExecutor {
	executor := &PeriodicalExecutor{
		// buffer 1 to let the caller go quickly
		commander:   make(chan interface{}, 1),
		interval:    interval,
		container:   container,
		confirmChan: make(chan utils.PlaceholderType),
		newTicker: func(d time.Duration) *time.Ticker {
			return time.NewTicker(interval)
		},
	}

	utils.AddShutdownListener(func() {
		executor.Flush()
	})

	return executor
}

func (pe *PeriodicalExecutor) Add(task interface{}) {
	if vals, ok := pe.addAndCheck(task); ok {
		pe.commander <- vals
		<-pe.confirmChan
	}
}

func (pe *PeriodicalExecutor) Flush() bool {
	pe.enterExecution()
	return pe.executeTasks(func() interface{} {
		pe.lock.Lock()
		defer pe.lock.Unlock()
		return pe.container.RemoveAll()
	}())
}

func (pe *PeriodicalExecutor) Sync(fn func()) {
	pe.lock.Lock()
	defer pe.lock.Unlock()
	fn()
}

func (pe *PeriodicalExecutor) Wait() {
	pe.wgBarrier.Guard(func() {
		pe.waitGroup.Wait()
	})
}

func (pe *PeriodicalExecutor) addAndCheck(task interface{}) (interface{}, bool) {
	pe.lock.Lock()
	defer func() {
		var start bool
		if !pe.guarded {
			pe.guarded = true
			start = true
		}
		pe.lock.Unlock()
		if start {
			pe.backgroundFlush()
		}
	}()

	if pe.container.AddTask(task) {
		return pe.container.RemoveAll(), true
	}

	return nil, false
}

func (pe *PeriodicalExecutor) backgroundFlush() {
	go service.RunSafe(func() {
		ticker := pe.newTicker(pe.interval)
		defer ticker.Stop()

		var commanded bool
		last := utils.Now()
		for {
			select {
			case vals := <-pe.commander:
				commanded = true
				pe.enterExecution()
				pe.confirmChan <- utils.Placeholder
				pe.executeTasks(vals)
				last = utils.Now()
			case <-ticker.C:
				if commanded {
					commanded = false
				} else if pe.Flush() {
					last = utils.Now()
				} else if utils.Since(last) > pe.interval*idleRound {
					pe.lock.Lock()
					pe.guarded = false
					pe.lock.Unlock()

					// flush again to avoid missing tasks
					pe.Flush()
					return
				}
			}
		}
	})
}

func (pe *PeriodicalExecutor) doneExecution() {
	pe.waitGroup.Done()
}

func (pe *PeriodicalExecutor) enterExecution() {
	pe.wgBarrier.Guard(func() {
		pe.waitGroup.Add(1)
	})
}

func (pe *PeriodicalExecutor) executeTasks(tasks interface{}) bool {
	defer pe.doneExecution()

	ok := pe.hasTasks(tasks)
	if ok {
		pe.container.Execute(tasks)
	}

	return ok
}

func (pe *PeriodicalExecutor) hasTasks(tasks interface{}) bool {
	if tasks == nil {
		return false
	}

	val := reflect.ValueOf(tasks)
	switch val.Kind() {
	case reflect.Array, reflect.Chan, reflect.Map, reflect.Slice:
		return val.Len() > 0
	default:
		// unknown type, let caller execute it
		return true
	}
}
