package server

import (
	"sync"
)

// TimestampedCounts track the total count of processed messages for a list of pods at a given timestamp
type TimestampedCounts struct {
	// timestamp in seconds, is the time when the count is recorded
	timestamp int64
	// podName to count mapping
	podCounts map[string]float64
	lock      *sync.RWMutex
}

func NewTimestampedCounts(t int64) *TimestampedCounts {
	return &TimestampedCounts{
		timestamp: t,
		podCounts: make(map[string]float64),
		lock:      new(sync.RWMutex),
	}
}

func (tc *TimestampedCounts) Update(podName string, count float64) {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	if count == CountNotAvailable {
		delete(tc.podCounts, podName)
		return
	}
	tc.podCounts[podName] = count
}

func (tc *TimestampedCounts) Snapshot() map[string]float64 {
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	counts := make(map[string]float64)
	for k, v := range tc.podCounts {
		counts[k] = v
	}
	return counts
}
