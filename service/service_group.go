package service

import (
	"log"
	"sync"

	"go-transfer/utils"
)

type (
	Starter interface {
		Start()
	}

	Stopper interface {
		Stop()
	}

	Service interface {
		Starter
		Stopper
	}

	ServiceGroup struct {
		services []Service
		stopOnce func()
	}
)

func NewServiceGroup() *ServiceGroup {
	sg := new(ServiceGroup)
	once := new(sync.Once)

	sg.stopOnce = func() {
		once.Do(sg.doStop)
	}
	return sg
}

func (sg *ServiceGroup) Add(service Service) {
	sg.services = append(sg.services, service)
}

func (sg *ServiceGroup) Start() {
	utils.AddShutdownListener(func() {
		log.Println("Shutting down...")
		sg.stopOnce()
	})

	sg.doStart()
}

func (sg *ServiceGroup) Stop() {
	log.Println("service group stop")
	sg.stopOnce()
}

func (sg *ServiceGroup) doStart() {
	routineGroup := NewRoutineGroup()

	for i := range sg.services {
		service := sg.services[i]
		routineGroup.RunSafe(func() {
			service.Start()
		})
	}

	routineGroup.Wait()
}

func (sg *ServiceGroup) doStop() {
	for _, service := range sg.services {
		service.Stop()
	}
}
