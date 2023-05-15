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

// fixedLookbackSeconds Always maintain rate metrics for the following lookback seconds (1m, 5m, 15m)
var fixedLookbackSeconds = map[string]int64{"1m": 60, "5m": 300, "15m": 900}

// TimestampedCount is a helper struct to wrap a count number and timestamp pair
type TimestampedCount struct {
	// count is the number of messages processed
	count float64
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
				newLatestSawTime := time.Now().Unix()
				orc.log.Infof("Start updating the timestamped total counts...")
				for _, v := range orc.pipeline.Spec.Vertices {
					// assuming countTracker, podTracker and rateTracker start at the same time approximately.
					// so not really using the timestamp in the countTracker yet.

					// get the latest count from the count tracker
					c := orc.countTracker.GetCounts(v.Name)
					delta := 0.0
					for _, v := range c {
						if v.latestSawCount.count == CountNotAvailable {
							continue
						}
						if v.prevSawCount.count == CountNotAvailable || v.latestSawCount.count < v.prevSawCount.count {
							delta += v.latestSawCount.count
							continue
						}
						delta += v.latestSawCount.count - v.prevSawCount.count
					}

					orc.log.Infof("Vertex %s has delta %f", v.Name, delta)

					if delta == 0 && orc.timestampedTotalCounts[v.Name].Length() == 0 {
						// if delta is 0 and the queue is empty, we don't need to update the queue
						// because the pipeline is still collecting data.
						// this also prevent the issue of getting unrealistic high rates when the pipeline is just started.
						continue
					}

					var newTotalCount float64
					if orc.timestampedTotalCounts[v.Name].Length() == 0 {
						newTotalCount = delta
					} else {
						newTotalCount = orc.timestampedTotalCounts[v.Name].Newest().count + delta
					}
					orc.timestampedTotalCounts[v.Name].Append(TimestampedCount{count: newTotalCount, timestamp: newLatestSawTime})
				}
				orc.log.Infof("Finished updating the timestamped total counts")
				for k, v := range orc.timestampedTotalCounts {
					for _, i := range v.Items() {
						orc.log.Infof("Vertex %s has timestamped total count %f at timestamp %d", k, i.count, i.timestamp)
					}
				}
			}
		}
	}()
	return nil
}

// GetRates returns the processing rates of the vertex for the given lookback seconds (1, 5, and 15m).
func (orc *OptimizedRateCalculator) GetRates(vn string) map[string]float64 {
	res := make(map[string]float64)
	// calculate rates for each lookback seconds
	// TODO - add user specified lookback seconds
	for n, i := range fixedLookbackSeconds {
		r := CalculateRate(orc.timestampedTotalCounts[vn], i)
		res[n] = r
	}
	return res
}
