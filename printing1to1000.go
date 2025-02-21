package main

import (
	"fmt"
	"sync"
)

func main() {
	var printed int
	printed = 1
	var wg sync.WaitGroup
	cond := sync.NewCond(&sync.Mutex{})
	for i := 1; i <= 1000; i++ {
		wg.Add(1)
		go func(num int) {
			cond.L.Lock()
			for printed != num { //
				cond.Wait() // block all goroutines who are not equal to printed value. Keep them alive using for loop.
			}
			printed++
			fmt.Printf("num: %d\n", num)
			cond.L.Unlock()
			cond.Broadcast() // Unblock all goroutines from waiting
			wg.Done()
		}(i)
	}
	wg.Wait()
}
