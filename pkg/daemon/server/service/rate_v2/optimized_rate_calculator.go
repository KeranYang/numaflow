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
	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

// TimestampedCount is a helper struct to wrap a count number and timestamp pair
type TimestampedCount struct {
	// count is the number of messages processed
	count int64
	// timestamp in seconds, is the time when the count is recorded
	timestamp int64
}

// OptimizedRateCalculator is a struct that calculates the processing rate of a vertex.
type OptimizedRateCalculator struct {
	pipeline     *v1alpha1.Pipeline
	log          *zap.SugaredLogger
	countTracker *CountTracker
	// vertex name to timestampedTotalCounts map
	timestampedTotalCounts map[string]*sharedqueue.OverflowQueue[TimestampedCount]
	refreshInterval        time.Duration
}

type Option func(*OptimizedRateCalculator)

// WithRefreshInterval sets how often to refresh the rate metrics.
func WithRefreshInterval(d time.Duration) Option {
	return func(r *OptimizedRateCalculator) {
		r.refreshInterval = d
	}
}

// NewOptimizedRateCalculator creates a new rate calculator.
func NewOptimizedRateCalculator(ctx context.Context, p *v1alpha1.Pipeline, countTracker *CountTracker, opts ...Option) *OptimizedRateCalculator {
	orc := OptimizedRateCalculator{
		pipeline:        p,
		log:             logging.FromContext(ctx).Named("OptimizedRateCalculator"),
		countTracker:    countTracker,
		refreshInterval: 5 * time.Second, // Default refresh interval
	}
	orc.timestampedTotalCounts = make(map[string]*sharedqueue.OverflowQueue[TimestampedCount])
	for _, v := range p.Spec.Vertices {
		// maintain the total counts of the last 30 minutes since we support 1m, 5m, 15m lookback seconds.
		orc.timestampedTotalCounts[v.Name] = sharedqueue.New[TimestampedCount](1800)
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&orc)
		}
	}
	return &orc
}

// Start TODO: description
func (orc *OptimizedRateCalculator) Start(ctx context.Context) error {
	orc.log.Infof("Starting rate calculator for pipeline %s...", orc.pipeline.Name)
	go func() {
		ticker := time.NewTicker(orc.refreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// TODO - implement it
				// update the timestampedTotalCounts map
			}
		}
	}()
	return nil
}

// GetRates returns the processing rates of the vertex for the given lookback seconds.
func (orc *OptimizedRateCalculator) GetRates(vn string) map[string]float64 {
	// TODO - implement it
	_ = vn
	return nil
}
