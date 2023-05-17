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
	"testing"

	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

func TestUpdateCount(t *testing.T) {
	t.Run("given_TimeExistsPodExistsCountAvailable_whenUpdate_updatePodCount", func(t *testing.T) {
		q := sharedqueue.New[TimestampedCount](1800)
		q.Append(TimestampedCount{
			timestamp: 1,
			podCounts: map[string]float64{
				"pod1": 10.0,
			},
		})
		UpdateCount(q, 1, "pod1", 20.0)
		if items := q.Items(); len(items) != 1 || items[0].podCounts["pod1"] != 20.0 {
			t.Errorf("UpdateCount failed to update existing count")
		}
	})

	t.Run("given_TimeExistsPodNotExistsCountAvailable_whenUpdate_addPodCount", func(t *testing.T) {
		q := sharedqueue.New[TimestampedCount](1800)
		q.Append(TimestampedCount{
			timestamp: 1,
			podCounts: map[string]float64{
				"pod1": 20.0,
			},
		})
		UpdateCount(q, 1, "pod2", 10.0)
		if items := q.Items(); len(items) != 1 || items[0].podCounts["pod1"] != 20.0 || items[0].podCounts["pod2"] != 10.0 {
			t.Errorf("UpdateCount failed to add a new pod count to existing timestamp")
		}
	})

	t.Run("given_TimeExistsPodExistsCountNotAvailable_whenUpdate_removePod", func(t *testing.T) {
		q := sharedqueue.New[TimestampedCount](1800)
		q.Append(TimestampedCount{
			timestamp: 1,
			podCounts: map[string]float64{
				"pod1": 10.0,
			},
		})
		UpdateCount(q, 1, "pod1", CountNotAvailable)
		if items := q.Items(); len(items) != 1 {
			t.Errorf("UpdateCount failed to remove a pod from existing timestamp")
		} else if _, ok := items[0].podCounts["pod1"]; ok {
			t.Errorf("UpdateCount failed to remove a pod from existing timestamp")
		}
	})

	t.Run("given_TimeExistsPodNotExistsCountNotAvailable_whenUpdate_noUpdate", func(t *testing.T) {
		q := sharedqueue.New[TimestampedCount](1800)
		q.Append(TimestampedCount{
			timestamp: 1,
			podCounts: map[string]float64{
				"pod1": 10.0,
			},
		})
		UpdateCount(q, 1, "pod2", CountNotAvailable)
		if items := q.Items(); len(items) != 1 || items[0].podCounts["pod1"] != 10.0 {
			t.Errorf("UpdateCount failed not to update the existing counts")
		}
	})

	t.Run("given_TimeNotExistsCountAvailable_whenUpdate_updateNewTimeWithPod", func(t *testing.T) {
		q := sharedqueue.New[TimestampedCount](1800)
		q.Append(TimestampedCount{
			timestamp: 1,
			podCounts: map[string]float64{
				"pod1": 10.0,
			},
		})
		UpdateCount(q, 2, "pod1", 20.0)
		if items := q.Items(); len(items) != 2 || items[0].podCounts["pod1"] != 10.0 || items[1].podCounts["pod1"] != 20.0 {
			t.Errorf("UpdateCount failed to add a new timestamp with pod count")
		}
	})

	t.Run("given_TimeNotExistsCountNotAvailable_whenUpdate_noUpdate", func(t *testing.T) {
		q := sharedqueue.New[TimestampedCount](1800)
		q.Append(TimestampedCount{
			timestamp: 1,
			podCounts: map[string]float64{
				"pod1": 10.0,
			},
		})
		UpdateCount(q, 2, "pod2", CountNotAvailable)
		if items := q.Items(); len(items) != 1 || items[0].podCounts["pod1"] != 10.0 {
			t.Errorf("UpdateCount failed not to update the existing counts")
		}
	})
}
