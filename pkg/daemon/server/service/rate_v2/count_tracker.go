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
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/shared/logging"
)

type void struct{}

var member void

type PodCount struct {
	name           string
	prevSawCount   TimestampedCount
	latestSawCount TimestampedCount
}

type CountTracker struct {
	log             *zap.SugaredLogger
	podTracker      *PodTracker
	tracker         map[PodCount]void
	refreshInterval time.Duration
}

// NewCountTracker creates...
func NewCountTracker(ctx context.Context, podTracker *PodTracker) *CountTracker {
	ct := CountTracker{
		log:             logging.FromContext(ctx).Named("OptimizedRateCalculator"),
		podTracker:      podTracker,
		tracker:         make(map[PodCount]void),
		refreshInterval: 5 * time.Second, // Default refresh interval
	}
	return &ct
}

// Start TODO: description
func (ct *CountTracker) Start(ctx context.Context) error {
	ct.log.Infof("Starting count tracker...")
	go func() {
		ticker := time.NewTicker(ct.refreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// TODO - implement it
				// update the tracker map
				podCount := PodCount{
					name:           "test",
					prevSawCount:   TimestampedCount{100, time.Now().Unix()},
					latestSawCount: TimestampedCount{80, time.Now().Unix()},
				}
				ct.tracker[podCount] = member
			}
		}
	}()
	return nil
}

func (ct *CountTracker) GetCounts(vertexName string) map[PodCount]void {
	// TODO - implement it
	_ = vertexName
	return nil
}
