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

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

type PodTracker struct {
	pipeline *v1alpha1.Pipeline
	log      *zap.SugaredLogger
	// a set of active pods' names
	activePods      map[string]void
	refreshInterval time.Duration
}

func NewPodTracker(ctx context.Context, p *v1alpha1.Pipeline) *PodTracker {
	pt := PodTracker{
		pipeline:        p,
		log:             logging.FromContext(ctx).Named("OptimizedRateCalculator"),
		refreshInterval: 5 * time.Second, // Default refresh interval
	}
	return &pt
}

// Start TODO: description
func (pt *PodTracker) Start(ctx context.Context) error {
	pt.log.Infof("Starting pod tracker for pipeline %s...", pt.pipeline.Name)
	go func() {
		ticker := time.NewTicker(pt.refreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// TODO - implement it
				// update the active pod set
			}
		}
	}()
	return nil
}

func (pt *PodTracker) GetActivePods() map[string]void {
	// race condition is not considered, yet.
	return pt.activePods
}
