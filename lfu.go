package main

import "fmt"

type LFU struct {
	maxCapacity int
	startCache  *LinkedList
	endCache    *LinkedList
	frqmap      map[*LinkedList]int
	currentSize int
}

func createLFU(maxCapacity int) *LFU {
	return &LFU{
		maxCapacity: maxCapacity,
		currentSize: 0,
		frqmap:      make(map[*LinkedList]int),
	}
}

type LinkedList struct {
	next *LinkedList
	val  any
}

func (lfu *LFU) incrementSize() {
	lfu.currentSize += 1
}

func (lfu *LFU) decrementSize() {
	lfu.currentSize -= 1
}

func (lfu *LFU) addCache(cache *LinkedList) {
	if lfu.currentSize == 0 {
		lfu.startCache = cache
		lfu.endCache = cache
		lfu.incrementSize()
		lfu.frqmap[cache] = 1
		fmt.Printf("Added new cache:%v with frequency: %d\n", cache.val, 1)

	} else if lfu.currentSize <= lfu.maxCapacity {
		if frq, ok := lfu.frqmap[cache]; !ok { // not found
			if lfu.currentSize == lfu.maxCapacity {
				lfu.decrementSize()
				lfu.removeCache()
			}
			lfu.frqmap[cache] = 1
			lfu.incrementSize()
			lfu.endCache.next = cache
			lfu.endCache = cache
			fmt.Printf("Added new cache:%v with frequency: %d\n", cache.val, lfu.frqmap[cache])

		} else if ok {
			lfu.frqmap[cache] = frq + 1
			lfu.rearrangement(cache)
			// rearrangement will be here
			fmt.Printf("Arranged existing cache:%v with frequency: %d\n", cache.val, lfu.frqmap[cache])
		}
	} else {
		// We can remove the least frequently used cache from tail
		// fmt.Printf("Can't add new cache:%v\n", cache.val)
		lfu.decrementSize()
		lfu.removeCache()
	}
	fmt.Printf("Current size of LFU: %d\n", lfu.currentSize)
	lfu.showAllCaches()
}
func (lfu *LFU) removeCache() {
	startNode := lfu.startCache
	for {
		if startNode.next == lfu.endCache {
			lfu.endCache = startNode // tail node is the end
			startNode.next = nil
			break
		} else {
			startNode = startNode.next
		}
	}
}

func (lfu *LFU) rearrangement(cache *LinkedList) {
	cur := lfu.startCache
	var prev *LinkedList
	prev = &LinkedList{
		val:  -1,
		next: cur,
	}
	dummy := prev

	if cur == cache { // startnode is the changed frequency cache
		// don't do anything
		return
	}
	targetFrq := lfu.frqmap[cache]

	for {
		if cur == nil {
			break
		}
		if cur.next != cache && lfu.frqmap[cur] >= targetFrq {
			prev = prev.next
			// next = cur.next
			cur = cur.next
		} else {
			break
		}
	}

	if lfu.endCache == cache {
		lfu.endCache = cur
	}
	prev.next = cache
	tmp := cache.next
	cache.next = cur
	cur.next = tmp
	lfu.startCache = dummy.next
}

func (lfu *LFU) showAllCaches() {
	cur := lfu.startCache
	for {
		if cur != nil {
			fmt.Printf("%d(%d)->", cur.val, lfu.frqmap[cur])
			cur = cur.next
		} else {
			break
		}
	}
	fmt.Println()
}

// least frequently used to be at the end of the list remove from tail
func main() {
	lfu := createLFU(3)
	cache1 := &LinkedList{
		val: 1,
	}
	cache2 := &LinkedList{
		val: 2,
	}
	cache3 := &LinkedList{
		val: 3,
	}
	cache4 := &LinkedList{
		val: 4,
	}
	lfu.addCache(cache1)
	lfu.addCache(cache2)
	lfu.addCache(cache3)
	lfu.addCache(cache2)
	lfu.addCache(cache1)
	lfu.addCache(cache2)
	lfu.addCache(cache3)
	lfu.addCache(cache4)
}
