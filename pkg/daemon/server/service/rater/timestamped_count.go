package server

import (
	"sync"
)

// TimestampedCount track the total count of processed messages for a list of pods at a given timestamp
type TimestampedCount struct {
	// timestamp in seconds, is the time when the count is recorded
	timestamp int64
	// podName to count mapping
	podCounts map[string]float64
	lock      *sync.RWMutex
}

func NewTimestampedCount(t int64) *TimestampedCount {
	return &TimestampedCount{
		timestamp: t,
		podCounts: make(map[string]float64),
		lock:      new(sync.RWMutex),
	}
}

func (tc *TimestampedCount) Remove(podName string) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	delete(tc.podCounts, podName)
}

func (tc *TimestampedCount) Update(podName string, count float64) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	tc.podCounts[podName] = count
}

func (tc *TimestampedCount) Snapshot() map[string]float64 {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	counts := make(map[string]float64)
	for k, v := range tc.podCounts {
		counts[k] = v
	}
	return counts
}
