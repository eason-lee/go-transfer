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
	// TaskContainer A type that satisfies executors.TaskContainer can be used as the underlying
	// container that used to do periodical executions.
	TaskContainer interface {
		// AddTask adds the task into the container.
		// Returns true if the container needs to be flushed after the addition.
		AddTask(task interface{}) bool
		// Execute handles the collected tasks by the container when flushing.
		Execute(tasks interface{})
		// RemoveAll remove 所有的 tasks 并返回 tasks
		RemoveAll() interface{}
	}

	// Barrier 锁
	Barrier struct {
		lock sync.Mutex
	}
	// PeriodicalExecutor 时间执行器
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

// NewPeriodicalExecutor ...
func NewPeriodicalExecutor(interval time.Duration, container TaskContainer) *PeriodicalExecutor {
	executor := &PeriodicalExecutor{
		commander:   make(chan interface{}, 1),
		interval:    interval,
		container:   container,
		confirmChan: make(chan utils.PlaceholderType),
		newTicker: func(d time.Duration) *time.Ticker {
			return time.NewTicker(interval)
		},
	}

	// 退出程序时， flush 数据
	utils.AddShutdownListener(func() {
		executor.Flush()
	})

	return executor
}

// Add ...
func (pe *PeriodicalExecutor) Add(task interface{}) {
	if vals, ok := pe.addAndCheck(task); ok {
		pe.commander <- vals
		<-pe.confirmChan
	}
}

// Flush ...
func (pe *PeriodicalExecutor) Flush() bool {
	pe.enterExecution()
	return pe.executeTasks(func() interface{} {
		pe.lock.Lock()
		defer pe.lock.Unlock()
		return pe.container.RemoveAll()
	}())
}

// Wait ...
func (pe *PeriodicalExecutor) Wait() {
	pe.wgBarrier.Guard(func() {
		pe.waitGroup.Wait()
	})
}

// addAndCheck 添加数据并检查是否执行的条件
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
				// 加锁
				pe.enterExecution()
				pe.confirmChan <- utils.Placeholder
				// 执行待执行的任务
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

// Guard 加锁执行函数
func (b *Barrier) Guard(fn func()) {
	b.lock.Lock()
	defer b.lock.Unlock()
	fn()
}
