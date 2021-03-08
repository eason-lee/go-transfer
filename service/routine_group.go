package service

import (
	"log"
	"sync"
)

type RoutineGroup struct {
	waitGroup sync.WaitGroup
}

func (g *RoutineGroup) Wait() {
	g.waitGroup.Wait()
}

func NewRoutineGroup() *RoutineGroup {
	return new(RoutineGroup)
}

func (g *RoutineGroup) Run(fn func()) {
	g.waitGroup.Add(1)

	go func() {
		defer g.waitGroup.Done()
		fn()
	}()
}

func RunSafe(fn func()) {
	defer func() {
		if p := recover(); p != nil {
			log.Println(p)
		}
	}()

	fn()
}

func (g *RoutineGroup) RunSafe(fn func()) {
	g.waitGroup.Add(1)

	go RunSafe(func() {
		defer g.waitGroup.Done()
		fn()
	})
}
