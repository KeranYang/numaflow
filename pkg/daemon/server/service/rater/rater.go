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
	"container/list"
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/common/expfmt"
	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

// metricsHttpClient interface for the GET/HEAD call to metrics endpoint.
// Had to add this an interface for testing
type metricsHttpClient interface {
	Get(url string) (*http.Response, error)
	Head(url string) (*http.Response, error)
}

type TimestampedCount struct {
	// timestamp in seconds, is the time when the count is recorded
	// it's a 10-second interval, so the timestamp is the end of the interval
	timestamp int64
	// podName -> count
	podCounts map[string]float64
}

// CountNotAvailable indicates that the rate calculator was not able to retrieve the count
const CountNotAvailable = 0.0

// fixedLookbackSeconds Always maintain rate metrics for the following lookback seconds (1m, 5m, 15m)
var fixedLookbackSeconds = map[string]int64{"1m": 60, "5m": 300, "15m": 900}

type Rater struct {
	pipeline   *v1alpha1.Pipeline
	httpClient metricsHttpClient
	log        *zap.SugaredLogger

	podList *list.List
	lock    *sync.RWMutex
	// vertexName -> TimestampedCounts
	timestampedPodCounts map[string]*sharedqueue.OverflowQueue[TimestampedCount]
	options              *options
}

func NewRater(ctx context.Context, p *v1alpha1.Pipeline, opts ...Option) *Rater {
	rater := Rater{
		pipeline: p,
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: time.Second * 1,
		},
		log:                  logging.FromContext(ctx).Named("Rater"),
		podList:              list.New(),
		timestampedPodCounts: make(map[string]*sharedqueue.OverflowQueue[TimestampedCount]),
		lock:                 new(sync.RWMutex),
		options:              defaultOptions(),
	}

	// initialize pod list and timestamped pod counts
	for _, v := range p.Spec.Vertices {
		var vertexType string
		if v.IsReduceUDF() {
			vertexType = "reduce"
		} else {
			vertexType = "non_reduce"
		}
		// start from simple, maximum 100 pods per vertex
		// TODO - most of the time we don't have 100 pods in a vertex, pending optimization on this for loop.
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("%s*%s*%d*%s", rater.pipeline.Name, v.Name, i, vertexType)
			rater.podList.PushBack(key)
		}
		// maintain the total counts of the last 30 minutes since we support 1m, 5m, 15m lookback seconds.
		rater.timestampedPodCounts[v.Name] = sharedqueue.New[TimestampedCount](1800)
	}

	for _, opt := range opts {
		if opt != nil {
			opt(rater.options)
		}
	}
	return &rater
}

// Function monitor() defines each of the worker's job.
// It waits for keys in the channel, and starts a scaling job
func (r *Rater) monitor(ctx context.Context, id int, keyCh <-chan string) {
	log := logging.FromContext(ctx)
	log.Infof("Started monitoring worker %v", id)
	for {
		select {
		case <-ctx.Done():
			log.Infof("Stopped monitoring worker %v", id)
			return
		case key := <-keyCh:
			if err := r.monitorOnePod(ctx, key, id); err != nil {
				log.Errorw("Failed to monitor a pod", zap.String("pod", key), zap.Error(err))
			}
		}
	}
}

func (r *Rater) monitorOnePod(ctx context.Context, key string, worker int) error {
	log := logging.FromContext(ctx).With("worker", fmt.Sprint(worker)).With("podKey", key)
	log.Infof("Working on key: %s", key)
	strs := strings.Split(key, "*")
	if len(strs) != 4 {
		return fmt.Errorf("invalid key %q", key)
	}
	vertexName := strs[1]
	vertexType := strs[3]
	podName := strs[0] + "-" + strs[1] + "-" + strs[2]
	var count float64
	if r.podExists(vertexName, podName) {
		count = r.getTotalCount(ctx, vertexName, vertexType, podName)
	} else {
		log.Infof("Pod %s does not exist, updating it with count 0.", podName)
		count = CountNotAvailable
	}
	r.updateCount(ctx, vertexName, podName, count)
	return nil
}

// Start starts the rate calculator that periodically fetches the total counts, calculates and updates the rates.
func (r *Rater) Start(ctx context.Context) error {
	r.log.Infof("Starting rater...")
	keyCh := make(chan string)
	ctx, cancel := context.WithCancel(logging.WithLogger(ctx, r.log))
	defer cancel()

	// Worker group
	for i := 1; i <= r.options.workers; i++ {
		go r.monitor(ctx, i, keyCh)
	}

	// Function assign() moves an element in the list from the front to the back,
	// and send to the channel so that it can be picked up by a worker.
	assign := func() {
		r.lock.Lock()
		defer r.lock.Unlock()
		if r.podList.Len() == 0 {
			return
		}
		e := r.podList.Front()
		if key, ok := e.Value.(string); ok {
			r.podList.MoveToBack(e)
			keyCh <- key
		}
	}

	// Following for loop keeps calling assign() function to assign monitoring tasks to the workers.
	// It makes sure each element in the list will be assigned every N milliseconds.
	for {
		select {
		case <-ctx.Done():
			r.log.Info("Shutting down rater job assigner")
			return nil
		default:
			assign()
		}
		// Make sure each of the key will be assigned at least every taskInterval milliseconds.
		time.Sleep(time.Millisecond * time.Duration(func() int {
			l := r.podList.Len()
			if l == 0 {
				return r.options.taskInterval
			}
			result := r.options.taskInterval / l
			if result > 0 {
				return result
			}
			return 1
		}()))
	}
}

func (r *Rater) podExists(vertexName, podName string) bool {
	// using the vertex headless service to check if a pod exists or not.
	// example for 0th pod : https://simple-pipeline-in-0.simple-pipeline-in-headless.default.svc.cluster.local:2469/metrics
	url := fmt.Sprintf("https://%s.%s.%s.svc.cluster.local:%v/metrics", podName, r.pipeline.Name+"-"+vertexName+"-headless", r.pipeline.Namespace, v1alpha1.VertexMetricsPort)
	if _, err := r.httpClient.Head(url); err != nil {
		return false
	}
	return true
}

// getTotalCount returns the total number of messages read by the pod
func (r *Rater) getTotalCount(ctx context.Context, vertexName, vertexType, podName string) float64 {
	log := logging.FromContext(ctx).Named("RateCalculator")
	// scrape the read total metric from pod metric port
	url := fmt.Sprintf("https://%s.%s.%s.svc.cluster.local:%v/metrics", podName, r.pipeline.Name+"-"+vertexName+"-headless", r.pipeline.Namespace, v1alpha1.VertexMetricsPort)
	if res, err := r.httpClient.Get(url); err != nil {
		log.Errorf("failed reading the metrics endpoint, %v", err.Error())
		return CountNotAvailable
	} else {
		textParser := expfmt.TextParser{}
		result, err := textParser.TextToMetricFamilies(res.Body)
		if err != nil {
			log.Errorf("failed parsing to prometheus metric families, %v", err.Error())
			return CountNotAvailable
		}

		var readTotalMetricName string
		if vertexType == "reduce" {
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

func (r *Rater) updateCount(_ context.Context, vertexName, podName string, count float64) {
	// track the total counts using a 10-second time window
	now := time.Now().Add(time.Second * 10).Truncate(time.Second * 10).Unix()
	vertexQueue := r.timestampedPodCounts[vertexName]

	// if the queue is empty or the latest element is before now, add the new element to the queue
	if vertexQueue.Length() == 0 || vertexQueue.Newest().timestamp < now {
		counts := make(map[string]float64)
		counts[podName] = count
		vertexQueue.Append(TimestampedCount{
			timestamp: now,
			podCounts: counts,
		})
		return
	}

	// if the latest element is at the same time as now, update the latest element
	if vertexQueue.Newest().timestamp == now {
		counts := vertexQueue.Newest().podCounts
		counts[podName] = count
		return
	}

	// if the latest element is after now, it means we need to update a previous element
	// find the element that is at the same time as now
	for _, i := range vertexQueue.Items() {
		if i.timestamp == now {
			counts := i.podCounts
			counts[podName] = count
			return
		}
	}
}

// GetRates returns the processing rates of the vertex in the format of lookback second to rate mappings
func (r *Rater) GetRates(vertexName string) map[string]float64 {
	var result = make(map[string]float64)
	lookbackSecondsMap := map[string]int64{}
	for k, v := range fixedLookbackSeconds {
		lookbackSecondsMap[k] = v
	}
	// calculate rates for each lookback seconds
	for n, i := range lookbackSecondsMap {
		r := CalculateRate(r.timestampedPodCounts[vertexName], i)
		result[n] = r
	}
	return result
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
