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

/*
func TestUpdateCount(t *testing.T) {
	t.Run("givenTimeExistsPodExistsCountAvailable_whenUpdate_thenUpdatePodCount", func(t *testing.T) {
		q := sharedqueue.New[TimestampedCounts](1800)
		q.Append(TimestampedCounts{
			timestamp: 1,
			podCounts: map[string]float64{
				"pod1": 10.0,
			},
			lock: new(sync.RWMutex),
		})
		UpdateCount(q, 1, "pod1", 20.0)
		if items := q.Items(); len(items) != 1 || items[0].podCounts["pod1"] != 20.0 {
			t.Errorf("UpdateCount failed to update existing count")
		}
	})

	t.Run("givenTimeExistsPodNotExistsCountAvailable_whenUpdate_thenAddPodCount", func(t *testing.T) {
		q := sharedqueue.New[TimestampedCounts](1800)
		q.Append(TimestampedCounts{
			timestamp: 1,
			podCounts: map[string]float64{
				"pod1": 20.0,
			},
			lock: new(sync.RWMutex),
		})
		UpdateCount(q, 1, "pod2", 10.0)
		if items := q.Items(); len(items) != 1 || items[0].podCounts["pod1"] != 20.0 || items[0].podCounts["pod2"] != 10.0 {
			t.Errorf("UpdateCount failed to add a new pod count to existing timestamp")
		}
	})

	t.Run("givenTimeExistsPodExistsCountNotAvailable_whenUpdate_thenRemovePod", func(t *testing.T) {
		q := sharedqueue.New[TimestampedCounts](1800)
		q.Append(TimestampedCounts{
			timestamp: 1,
			podCounts: map[string]float64{
				"pod1": 10.0,
			},
			lock: new(sync.RWMutex),
		})
		UpdateCount(q, 1, "pod1", CountNotAvailable)
		if items := q.Items(); len(items) != 1 {
			t.Errorf("UpdateCount failed to remove a pod from existing timestamp")
		} else if _, ok := items[0].podCounts["pod1"]; ok {
			t.Errorf("UpdateCount failed to remove a pod from existing timestamp")
		}
	})

	t.Run("givenTimeExistsPodNotExistsCountNotAvailable_whenUpdate_thenNoUpdate", func(t *testing.T) {
		q := sharedqueue.New[TimestampedCounts](1800)
		q.Append(TimestampedCounts{
			timestamp: 1,
			podCounts: map[string]float64{
				"pod1": 10.0,
			},
			lock: new(sync.RWMutex),
		})
		UpdateCount(q, 1, "pod2", CountNotAvailable)
		if items := q.Items(); len(items) != 1 || items[0].podCounts["pod1"] != 10.0 {
			t.Errorf("UpdateCount failed not to update the existing counts")
		}
	})

	t.Run("givenTimeNotExistsCountAvailable_whenUpdate_thenUpdateNewTimeWithPod", func(t *testing.T) {
		q := sharedqueue.New[TimestampedCounts](1800)
		q.Append(TimestampedCounts{
			timestamp: 1,
			podCounts: map[string]float64{
				"pod1": 10.0,
			},
			lock: new(sync.RWMutex),
		})
		UpdateCount(q, 2, "pod1", 20.0)
		if items := q.Items(); len(items) != 2 || items[0].podCounts["pod1"] != 10.0 || items[1].podCounts["pod1"] != 20.0 {
			t.Errorf("UpdateCount failed to add a new timestamp with pod count")
		}
	})

	t.Run("givenTimeNotExistsCountNotAvailable_whenUpdate_thenNoUpdate", func(t *testing.T) {
		q := sharedqueue.New[TimestampedCounts](1800)
		q.Append(TimestampedCounts{
			timestamp: 1,
			podCounts: map[string]float64{
				"pod1": 10.0,
			},
			lock: new(sync.RWMutex),
		})
		UpdateCount(q, 2, "pod2", CountNotAvailable)
		if items := q.Items(); len(items) != 1 || items[0].podCounts["pod1"] != 10.0 {
			t.Errorf("UpdateCount failed not to update the existing counts")
		}
	})
}

func TestCalculateRate(t *testing.T) {
	t.Run("givenCollectedTimeLessThanTwo_whenCalculateRate_thenReturnZero", func(t *testing.T) {
		q := sharedqueue.New[TimestampedCounts](1800)
		rate := CalculateRate(q, 10)
		assert.Equal(t, 0.0, rate)
	})

	t.Run("singlePod_givenCountIncreases_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[TimestampedCounts](1800)
		now := time.Now()
		q.Append(TimestampedCounts{
			timestamp: now.Truncate(time.Second*10).Unix() - 20,
			podCounts: map[string]float64{
				"pod1": 5.0,
			},
			lock: new(sync.RWMutex),
		})
		q.Append(TimestampedCounts{
			timestamp: now.Truncate(time.Second*10).Unix() - 10,
			podCounts: map[string]float64{
				"pod1": 10.0,
			},
			lock: new(sync.RWMutex),
		})
		q.Append(TimestampedCounts{
			timestamp: now.Truncate(time.Second * 10).Unix(),
			podCounts: map[string]float64{
				"pod1": 20.0,
			},
			lock: new(sync.RWMutex),
		})
		assert.Equal(t, 0.0, CalculateRate(q, 5))
		assert.Equal(t, 1.0, CalculateRate(q, 15))
		assert.Equal(t, 0.75, CalculateRate(q, 25))
		assert.Equal(t, 0.75, CalculateRate(q, 100))
	})

	t.Run("singlePod_givenCountDecreases_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[TimestampedCounts](1800)
		now := time.Now()
		q.Append(TimestampedCounts{
			timestamp: now.Truncate(time.Second*10).Unix() - 30,
			podCounts: map[string]float64{
				"pod1": 200.0,
			},
			lock: new(sync.RWMutex),
		})
		q.Append(TimestampedCounts{
			timestamp: now.Truncate(time.Second*10).Unix() - 20,
			podCounts: map[string]float64{
				"pod1": 100.0,
			},
			lock: new(sync.RWMutex),
		})
		q.Append(TimestampedCounts{
			timestamp: now.Truncate(time.Second*10).Unix() - 10,
			podCounts: map[string]float64{
				"pod1": 50.0,
			},
			lock: new(sync.RWMutex),
		})
		q.Append(TimestampedCounts{
			timestamp: now.Truncate(time.Second * 10).Unix(),
			podCounts: map[string]float64{
				"pod1": 80.0,
			},
			lock: new(sync.RWMutex),
		})
		assert.Equal(t, 0.0, CalculateRate(q, 5))
		assert.Equal(t, 3.0, CalculateRate(q, 15))
		assert.Equal(t, 4.0, CalculateRate(q, 25))
		assert.Equal(t, 6.0, CalculateRate(q, 35))
		assert.Equal(t, 6.0, CalculateRate(q, 100))
	})

	t.Run("multiplePods_givenCountIncreasesAndDecreases_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[TimestampedCounts](1800)
		now := time.Now()
		q.Append(TimestampedCounts{
			timestamp: now.Truncate(time.Second*10).Unix() - 30,
			podCounts: map[string]float64{
				"pod1": 200.0,
				"pod2": 100.0,
			},
			lock: new(sync.RWMutex),
		})
		q.Append(TimestampedCounts{
			timestamp: now.Truncate(time.Second*10).Unix() - 20,
			podCounts: map[string]float64{
				"pod1": 100.0,
				"pod2": 200.0,
			},
			lock: new(sync.RWMutex),
		})
		q.Append(TimestampedCounts{
			timestamp: now.Truncate(time.Second*10).Unix() - 10,
			podCounts: map[string]float64{
				"pod1": 50.0,
				"pod2": 300.0,
			},
			lock: new(sync.RWMutex),
		})
		q.Append(TimestampedCounts{
			timestamp: now.Truncate(time.Second * 10).Unix(),
			podCounts: map[string]float64{
				"pod1": 80.0,
				"pod2": 400.0,
			},
			lock: new(sync.RWMutex),
		})
		assert.Equal(t, 0.0, CalculateRate(q, 5))
		assert.Equal(t, 13.0, CalculateRate(q, 15))
		assert.Equal(t, 14.0, CalculateRate(q, 25))
		assert.Equal(t, 16.0, CalculateRate(q, 35))
		assert.Equal(t, 16.0, CalculateRate(q, 100))
	})

	t.Run("multiplePods_givenPodsComeAndGo_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[TimestampedCounts](1800)
		now := time.Now()
		q.Append(TimestampedCounts{
			timestamp: now.Truncate(time.Second*10).Unix() - 30,
			podCounts: map[string]float64{
				"pod1": 200.0,
				"pod2": 90.0,
				"pod3": 50.0,
			},
			lock: new(sync.RWMutex),
		})
		q.Append(TimestampedCounts{
			timestamp: now.Truncate(time.Second*10).Unix() - 20,
			podCounts: map[string]float64{
				"pod1": 100.0,
				"pod2": 200.0,
			},
			lock: new(sync.RWMutex),
		})
		q.Append(TimestampedCounts{
			timestamp: now.Truncate(time.Second*10).Unix() - 10,
			podCounts: map[string]float64{
				"pod1": 50.0,
				"pod2": 300.0,
				"pod4": 100.0,
			},
			lock: new(sync.RWMutex),
		})
		q.Append(TimestampedCounts{
			timestamp: now.Truncate(time.Second * 10).Unix(),
			podCounts: map[string]float64{
				"pod2":   400.0,
				"pod3":   200.0,
				"pod100": 200.0,
			},
			lock: new(sync.RWMutex),
		})
		assert.Equal(t, 0.0, CalculateRate(q, 5))
		assert.Equal(t, 50.0, CalculateRate(q, 15))
		assert.Equal(t, 37.5, CalculateRate(q, 25))
		assert.Equal(t, 32.0, CalculateRate(q, 35))
		assert.Equal(t, 32.0, CalculateRate(q, 100))
	})

	t.Run("queueOverflowed_SinglePod_givenCountIncreases_whenCalculateRate_thenReturnRate", func(t *testing.T) {
		q := sharedqueue.New[TimestampedCounts](3)
		now := time.Now()
		q.Append(TimestampedCounts{
			timestamp: now.Truncate(time.Second*10).Unix() - 30,
			podCounts: map[string]float64{
				"pod1": 200.0,
				"pod2": 90.0,
				"pod3": 50.0,
			},
			lock: new(sync.RWMutex),
		})
		q.Append(TimestampedCounts{
			timestamp: now.Truncate(time.Second*10).Unix() - 20,
			podCounts: map[string]float64{
				"pod1": 100.0,
				"pod2": 200.0,
			},
			lock: new(sync.RWMutex),
		})
		q.Append(TimestampedCounts{
			timestamp: now.Truncate(time.Second*10).Unix() - 10,
			podCounts: map[string]float64{
				"pod1": 50.0,
				"pod2": 300.0,
				"pod4": 100.0,
			},
			lock: new(sync.RWMutex),
		})
		q.Append(TimestampedCounts{
			timestamp: now.Truncate(time.Second * 10).Unix(),
			podCounts: map[string]float64{
				"pod2":   400.0,
				"pod3":   200.0,
				"pod100": 200.0,
			},
			lock: new(sync.RWMutex),
		})
		assert.Equal(t, 0.0, CalculateRate(q, 5))
		assert.Equal(t, 50.0, CalculateRate(q, 15))
		assert.Equal(t, 37.5, CalculateRate(q, 25))
		assert.Equal(t, 37.5, CalculateRate(q, 35))
		assert.Equal(t, 37.5, CalculateRate(q, 100))
	})
}


*/
