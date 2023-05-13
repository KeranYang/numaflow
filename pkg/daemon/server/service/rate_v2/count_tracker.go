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
	"crypto/tls"
	"fmt"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/common/expfmt"
	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// CountNotAvailable indicates that the rate calculator was not able to retrieve the count
const CountNotAvailable = float64(math.MinInt)

// TODO - rename this to something more meaningful
type PodCount struct {
	name           string
	prevSawCount   TimestampedCount
	latestSawCount TimestampedCount
}

type CountTracker struct {
	log             *zap.SugaredLogger
	pipeline        *v1alpha1.Pipeline
	httpClient      MetricsHttpClient
	podTracker      *PodTracker
	counts          map[string]*PodCount
	refreshInterval time.Duration
}

// NewCountTracker creates...
func NewCountTracker(ctx context.Context, p *v1alpha1.Pipeline, podTracker *PodTracker) *CountTracker {
	ct := CountTracker{
		log:      logging.FromContext(ctx).Named("CountTracker"),
		pipeline: p,
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			// TODO: adjust timeout
			Timeout: time.Second,
		},
		podTracker:      podTracker,
		counts:          make(map[string]*PodCount),
		refreshInterval: 5 * time.Second, // Default refresh interval
	}
	return &ct
}

// Start TODO: description
func (ct *CountTracker) Start(ctx context.Context) error {
	ct.log.Infof("Starting count counts...")
	go func() {
		ticker := time.NewTicker(ct.refreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				activePods := ct.podTracker.GetActivePods()
				newLatestSawTime := time.Now().Unix()

				// update the counts
				ct.log.Infof("Start updating the pod counts...")
				var wg sync.WaitGroup
				mu := sync.Mutex{}
				for _, v := range ct.pipeline.Spec.Vertices {
					var limit int
					if max := v.Scale.Max; max != nil {
						limit = int(*max)
					} else {
						// default max number of pods per vertex
						limit = 500
					}
					for i := 0; i < limit; i++ {
						wg.Add(1)
						go func(v v1alpha1.AbstractVertex, i int) {
							defer wg.Done()
							// update the counts map for each pod
							podName := fmt.Sprintf("%s-%s-%d", ct.pipeline.Name, v.Name, i)
							if _, ok := activePods[podName]; ok {
								// pod is active
								newCount := ct.getTotalCount(ctx, v, podName)
								mu.Lock()
								// if a pod is active, we look at the count tracker to see if we have seen this pod before
								if count := ct.counts[podName]; count != nil {
									// if we have seen this pod before, we update the latestSawCount and prevSawCount
									count.prevSawCount = count.latestSawCount
									count.latestSawCount = TimestampedCount{newCount, newLatestSawTime}
								} else {
									// if we have not seen this pod before, we add it to the count tracker
									ct.counts[podName] = &PodCount{
										name:           podName,
										prevSawCount:   TimestampedCount{CountNotAvailable, time.Unix(0, 0).Unix()},
										latestSawCount: TimestampedCount{newCount, newLatestSawTime},
									}
								}
								mu.Unlock()
							} else {
								// pod is not active, we don't bother looking for the count
								mu.Lock()
								// we look at the count tracker to see if we have seen this pod before
								if count := ct.counts[podName]; count != nil {
									// if we have seen this pod before, we update the latestSawCount and prevSawCount
									count.prevSawCount = count.latestSawCount
									count.latestSawCount = TimestampedCount{CountNotAvailable, time.Unix(0, 0).Unix()}
									if count.latestSawCount.count == CountNotAvailable && count.prevSawCount.count == CountNotAvailable {
										// remove it from the count tracker
										delete(ct.counts, podName)
									}
								}
								// if we have not seen this pod before, maybe this pod never exists
								// we don't bother adding it to the count tracker
								mu.Unlock()
							}
						}(v, i)
					}
				}
				wg.Wait()
				ct.log.Infof("Finished updating the counts...")
				for k, v := range ct.counts {
					ct.log.Infof("Pod: %s, prevSawCount: %v, latestSawCount: %v\n", k, v.prevSawCount, v.latestSawCount)
				}
			}
		}
	}()
	return nil
}

func (ct *CountTracker) GetCounts(vertexName string) map[string]PodCount {
	res := make(map[string]PodCount)
	for k, v := range ct.counts {
		if strings.Contains(k, vertexName) {
			res[k] = *v
		}
	}
	return res
}

// getTotalCount returns the total number of messages read by the pod
func (ct *CountTracker) getTotalCount(ctx context.Context, vertex v1alpha1.AbstractVertex, podName string) float64 {
	// scrape the read total metric from pod metric port
	url := fmt.Sprintf("https://%s.%s.%s.svc.cluster.local:%v/metrics", podName, ct.pipeline.Name+"-"+vertex.Name+"-headless", ct.pipeline.Namespace, v1alpha1.VertexMetricsPort)
	if res, err := ct.httpClient.Get(url); err != nil {
		ct.log.Infof("failed reading the metrics endpoint, %v", err.Error())
		return CountNotAvailable
	} else {
		textParser := expfmt.TextParser{}
		result, err := textParser.TextToMetricFamilies(res.Body)
		if err != nil {
			ct.log.Infof("failed parsing to prometheus metric families, %v", err.Error())
			return CountNotAvailable
		}

		var readTotalMetricName string
		if vertex.IsReduceUDF() {
			readTotalMetricName = "reduce_isb_reader_read_total"
		} else {
			readTotalMetricName = "forwarder_read_total"
		}

		if value, ok := result[readTotalMetricName]; ok && value != nil && len(value.GetMetric()) > 0 {
			metricsList := value.GetMetric()
			return metricsList[0].Counter.GetValue()
		} else {
			return CountNotAvailable
		}
	}
}
