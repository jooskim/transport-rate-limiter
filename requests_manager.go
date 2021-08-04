package ratelimiter

import (
	"sync"
	"time"
)

type timestampLinkedList struct {
	time *time.Time
	next *timestampLinkedList
}

type timestampsManager struct {
	channel string
	timestamps *timestampLinkedList
	timestampsLen int
	mu sync.Mutex
}

func (tm *timestampsManager) addTimestamp(t *time.Time) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if tm.timestamps == nil {
		tm.timestampsLen++
		tm.timestamps = &timestampLinkedList{
			time: t,
			next: nil,
		}
		return
	}
	// create a new history and put it at the root of the linked list
	newHistory := &timestampLinkedList{
		time: t,
		next: tm.timestamps,
	}
	tm.timestampsLen++
	tm.timestamps = newHistory
}

func (tm *timestampsManager) deleteTimestampOlderThan(duration time.Duration) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if tm.timestamps == nil {
		return
	}

	var prev, curr *timestampLinkedList
	curr = tm.timestamps
	now := time.Now()
	timestampsCount := 0
	for curr != nil {
		// no point in further inspecting timestamps older than curr. break out of the loop and set prev as new tm.timestamp
		if now.After(curr.time.Add(duration)) {
			break
		}
		timestampsCount++
		prev = curr
		curr = curr.next
	}

	if prev != nil {
		tm.timestampsLen = timestampsCount
		prev.next = nil
	}
}
