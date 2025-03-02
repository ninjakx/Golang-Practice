package main

import (
	"fmt"
	"sync"
)

var wg sync.WaitGroup

type Task struct {
	// dependencies
	dependencies []*Task
	quitSignal   chan bool
	subscription []chan bool // receive channel
	taskName     string
	subscribers  []*Task
}

func initTask(taskName string, subscribers int) *Task {
	return &Task{
		subscription: make([]chan bool, subscribers),
		taskName:     taskName,
		quitSignal:   make(chan bool, 1),
	}
}

func (t *Task) addDependency(dep *Task) {
	t.dependencies = append(t.dependencies, dep)
	dep.subscribers = append(dep.subscribers, t)
}

func (t *Task) sendStopSignal() {
	fmt.Printf("sending stop signal to %v\n", t.taskName)
	t.quitSignal <- true
	close(t.quitSignal)
}

func (t *Task) runTask() {
	for {
		select {
		case <-t.quitSignal:
			fmt.Printf("Done executing %v\n", t.taskName)
			wg.Done()
			return

		default:
			fmt.Printf("%v is running having dependency of %v tasks\n", t.taskName, len(t.dependencies))
			for _, dep := range t.dependencies {
				<-dep.quitSignal
			}
			t.quitTaskSignalToOthers()
		}
	}
}

func (t *Task) quitTaskSignalToOthers() {
	for _, dep := range t.subscribers {
		fmt.Printf("%v is done and notifying the subscriber %v\n", t.taskName, dep.taskName)
		done := make(chan bool, 1)
		done <- true
		dep.subscription = append(dep.subscription, done)
	}
	t.sendStopSignal()
}

func main() {
	t1 := initTask("Task1", 2)
	t2 := initTask("Task2", 0)
	t3 := initTask("Task3", 2)
	t4 := initTask("Task4", 0)
	t5 := initTask("Task5", 0)

	t1.addDependency(t2)
	t1.addDependency(t3)
	t3.addDependency(t4)
	t3.addDependency(t5)
	t2.addDependency(t4)

	tasks := []*Task{t1, t2, t3, t4, t5}

	for _, task := range tasks {
		wg.Add(1)
		go task.runTask()
	}
	wg.Wait()

}
