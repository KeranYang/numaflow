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
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

type void struct{}

var member void

// MetricsHttpClient interface for the GET/HEAD call to metrics endpoint.
// Had to add this an interface for testing
type MetricsHttpClient interface {
	Get(url string) (*http.Response, error)
	Head(url string) (*http.Response, error)
}

type PodTracker struct {
	pipeline   *v1alpha1.Pipeline
	log        *zap.SugaredLogger
	httpClient MetricsHttpClient
	// a set of active pods' names
	activePods      map[string]void
	refreshInterval time.Duration
}

func NewPodTracker(ctx context.Context, p *v1alpha1.Pipeline) *PodTracker {
	pt := PodTracker{
		pipeline: p,
		log:      logging.FromContext(ctx).Named("OptimizedRateCalculator"),
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			// TODO: adjust timeout
			Timeout: time.Second,
		},
		activePods:      make(map[string]void),
		refreshInterval: 5 * time.Second, // Default refresh interval
	}
	return &pt
}

// Start TODO: description
func (pt *PodTracker) Start(ctx context.Context) error {
	pt.log.Infof("Starting pod counts for pipeline %s...", pt.pipeline.Name)
	go func() {
		ticker := time.NewTicker(pt.refreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// update the active pod set
				pt.log.Infof("Start updating the active pod set...")
				var wg sync.WaitGroup
				mu := sync.Mutex{}
				for _, v := range pt.pipeline.Spec.Vertices {
					var limit int
					if max := v.Scale.Max; max != nil {
						limit = int(*max)
					} else {
						// default max number of pods per vertex
						limit = 500
					}
					for i := 0; i < limit; i++ {
						wg.Add(1)
						go func(v string, i int) {
							defer wg.Done()
							podName := fmt.Sprintf("%s-%s-%d", pt.pipeline.Name, v, i)
							if pt.isActive(v, podName) {
								mu.Lock()
								pt.activePods[podName] = member
								mu.Unlock()
							} else {
								mu.Lock()
								delete(pt.activePods, podName)
								mu.Unlock()
							}
						}(v.Name, i)
					}
				}
				wg.Wait()
				pt.log.Infof("Finished updating the active pod set: %v", pt.activePods)
			}
		}
	}()
	return nil
}

func (pt *PodTracker) GetActivePods() map[string]void {
	// race condition is not considered, yet.
	return pt.activePods
}

func (pt *PodTracker) isActive(vertexName, podName string) bool {
	// using the vertex headless service to check if a pod exists or not.
	// example for 0th pod : https://simple-pipeline-in-0.simple-pipeline-in-headless.default.svc.cluster.local:2469/metrics
	url := fmt.Sprintf("https://%s.%s.%s.svc.cluster.local:%v/metrics", podName, pt.pipeline.Name+"-"+vertexName+"-headless", pt.pipeline.Namespace, v1alpha1.VertexMetricsPort)
	if _, err := pt.httpClient.Head(url); err != nil {
		return false
	}
	return true
}
