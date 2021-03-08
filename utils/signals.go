// +build linux darwin

package utils

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"log"
)

var (
	shutdownListeners        = new(listenerManager)
	delayTimeBeforeForceQuit = 5 * time.Second
)

type listenerManager struct {
	lock      sync.Mutex
	waitGroup sync.WaitGroup
	listeners []func()
}

func (lm *listenerManager) addListener(fn func()) (waitForCalled func()) {
	lm.waitGroup.Add(1)

	lm.lock.Lock()
	lm.listeners = append(lm.listeners, func() {
		defer lm.waitGroup.Done()
		fn()
	})
	lm.lock.Unlock()

	return func() {
		lm.waitGroup.Wait()
	}
}

func (lm *listenerManager) notifyListeners() {
	lm.lock.Lock()
	defer lm.lock.Unlock()

	for _, listener := range lm.listeners {
		listener()
	}
}

// AddShutdownListener 注册退出信号的监听者，退出前执行注册的函数
func AddShutdownListener(fn func()) (waitForCalled func()) {
	return shutdownListeners.addListener(fn)
}

// 设置强制退出时的等待时间
func SetTimeToForceQuit(duration time.Duration) {
	delayTimeBeforeForceQuit = duration
}

func gracefulStop(signals chan os.Signal) {
	signal.Stop(signals)

	log.Println("Got signal SIGTERM, shutting down...")
	shutdownListeners.notifyListeners()

	time.Sleep(delayTimeBeforeForceQuit)
	log.Printf("Still alive after %v, going to force kill the process...", delayTimeBeforeForceQuit)
	syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
}

func init() {
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)

		for {
			v := <-signals
			switch v {
			case syscall.SIGTERM , syscall.SIGINT:
				gracefulStop(signals)
			}
		}
	}()
}
