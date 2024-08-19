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

package rater

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

type raterMockHttpClient struct {
	podOneCount int64
	podTwoCount int64
	lock        *sync.RWMutex
}

func (m *raterMockHttpClient) Get(url string) (*http.Response, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if url == "https://p-mv-0.p-mv-headless.default.svc:2469/metrics" {
		m.podOneCount = m.podOneCount + 20
		resp := &http.Response{
			StatusCode: 200,
			// we use the default monovertex forwarder metric name "monovtx_read_total" is used to retrieve the metric
			Body: io.NopCloser(bytes.NewReader([]byte(fmt.Sprintf(`
# HELP monovtx_read A Counter to keep track of the total number of messages read from the source.
# TYPE monovtx_read counter
monovtx_read_total{mvtx_name="simple-mono-vertex",mvtx_replica="0"} %d
`, m.podOneCount))))}
		return resp, nil
	} else if url == "https://p-mv-1.p-mv-headless.default.svc:2469/metrics" {
		m.podTwoCount = m.podTwoCount + 60
		resp := &http.Response{
			StatusCode: 200,
			Body: io.NopCloser(bytes.NewReader([]byte(fmt.Sprintf(`
# HELP monovtx_read A Counter to keep track of the total number of messages read from the source.
# TYPE monovtx_read counter
monovtx_read_total{mvtx_name="simple-mono-vertex",mvtx_replica="1"} %d
`, m.podTwoCount))))}
		return resp, nil
	} else {
		return nil, nil
	}
}

func (m *raterMockHttpClient) Head(url string) (*http.Response, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if url == "https://p-mv-0.p-mv-headless.default.svc:2469/metrics" {
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(bytes.NewReader([]byte(``)))}, nil
	} else if url == "https://p-mv-1.p-mv-headless.default.svc:2469/metrics" {
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(bytes.NewReader([]byte(``)))}, nil
	} else {
		return nil, fmt.Errorf("unknown url: %s", url)
	}
}

// TestRater_Start tests the rater by mocking the http client
// we mock the metrics endpoint of the pods and increment the read count by 20 for pod one, and 60 for pod two,
// then we verify that the rate calculator is able to calculate a positive rate for the vertex
// note: this test doesn't test the accuracy of the calculated rate, the calculation is tested by helper_test.go
func TestRater_Start(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*29)
	lookBackSeconds := uint32(30)
	defer cancel()
	pipeline := &v1alpha1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p",
			Namespace: "default",
		},
		Spec: v1alpha1.MonoVertexSpec{
			Scale: v1alpha1.Scale{LookbackSeconds: &lookBackSeconds},
		},
	}
	r := NewRater(ctx, pipeline, WithTaskInterval(1000))
	podTracker := NewPodTracker(ctx, pipeline, WithRefreshInterval(time.Second*1))
	podTracker.httpClient = &raterMockHttpClient{podOneCount: 0, podTwoCount: 0, lock: &sync.RWMutex{}}
	r.httpClient = &raterMockHttpClient{podOneCount: 0, podTwoCount: 0, lock: &sync.RWMutex{}}
	r.podTracker = podTracker

	timer := time.NewTimer(60 * time.Second)
	succeedChan := make(chan struct{})
	go func() {
		if err := r.Start(ctx); err != nil {
			log.Fatalf("failed to start rater: %v", err)
		}
	}()
	go func() {
		for {
			if r.GetRates()["default"].GetValue() <= 0 {
				time.Sleep(time.Second)
			} else {
				succeedChan <- struct{}{}
				break
			}
		}
	}()
	select {
	case <-succeedChan:
		time.Sleep(time.Second)
		break
	case <-timer.C:
		t.Fatalf("timed out waiting for rate to be calculated")
	}
	timer.Stop()
}
