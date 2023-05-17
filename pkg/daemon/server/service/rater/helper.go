/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package server

import (
	"time"

	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

func UpdateCount(q *sharedqueue.OverflowQueue[TimestampedCount], now int64, podName string, count float64) {
	// find the element that is at the same time as now and update it
	for _, i := range q.Items() {
		if i.timestamp == now {
			counts := i.podCounts
			if count == CountNotAvailable {
				// if the count is not available, we need to delete the pod from the map
				delete(counts, podName)
			} else {
				counts[podName] = count
			}
			return
		}
	}

	// if we cannot find an element that is at the same time as now, it means we need to add a new timestamped count to the queue
	if count != CountNotAvailable {
		counts := make(map[string]float64)
		counts[podName] = count
		q.Append(TimestampedCount{
			timestamp: now,
			podCounts: counts,
		})
	}
}

// CalculateRate calculates the rate of the vertex in the last lookback seconds
func CalculateRate(q *sharedqueue.OverflowQueue[TimestampedCount], lookbackSeconds int64) float64 {
	n := q.Length()
	if n <= 1 {
		return 0
	}

	now := time.Now().Truncate(time.Second * 10).Unix()
	counts := q.Items()
	var startIndex int
	startCountInfo := counts[n-2]
	if now-startCountInfo.timestamp > lookbackSeconds {
		return 0
	}
	for i := n - 3; i >= 0; i-- {
		if now-counts[i].timestamp <= lookbackSeconds {
			startIndex = i
		} else {
			break
		}
	}
	delta := float64(0)
	// time diff in seconds.
	timeDiff := counts[n-1].timestamp - counts[startIndex].timestamp
	for i := startIndex; i < n-1; i++ {
		delta = delta + calculateDelta(counts[i], counts[i+1])
	}
	return delta / float64(timeDiff)
}

func calculateDelta(c1, c2 TimestampedCount) float64 {
	delta := float64(0)
	// Iterate over the podCounts of the second TimestampedCount
	for pod, count2 := range c2.podCounts {
		// If the pod also exists in the first TimestampedCount
		if count1, ok := c1.podCounts[pod]; ok {
			// If the count has decreased, it means the pod restarted
			if count2 < count1 {
				delta += count2
			} else { // If the count has increased or stayed the same
				delta += count2 - count1
			}
		} else { // If the pod only exists in the second TimestampedCount, it's a new pod
			delta += count2
		}
	}
	return delta
}
