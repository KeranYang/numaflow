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

package e2e

import (
	"context"
	"fmt"
	daemonclient "github.com/numaproj/numaflow/pkg/daemon/client"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	. "github.com/numaproj/numaflow/test/fixtures"
	"github.com/stretchr/testify/suite"
)

//go:generate kubectl -n numaflow-system delete statefulset redis-cluster --ignore-not-found=true
//go:generate kubectl apply -k ../../config/apps/redis -n numaflow-system
// Wait for redis cluster to come up
//go:generate sleep 60

type FunctionalSuite struct {
	E2ESuite
}

func (s *FunctionalSuite) TestCreateSimplePipeline() {
	w := s.Given().Pipeline("@testdata/simple-pipeline.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()

	pipelineName := "simple-pipeline"

	w.Expect().
		VertexPodsRunning().DaemonPodsRunning().
		VertexPodLogContains("input", LogSourceVertexStarted).
		VertexPodLogContains("p1", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("output", LogSinkVertexStarted).
		DaemonPodLogContains(pipelineName, LogDaemonStarted).
		VertexPodLogContains("output", `"Data":".*","Createdts":.*`)

	defer w.VertexPodPortForward("input", 8001, dfv1.VertexMetricsPort).
		VertexPodPortForward("p1", 8002, dfv1.VertexMetricsPort).
		VertexPodPortForward("output", 8003, dfv1.VertexMetricsPort).
		DaemonPodPortForward(pipelineName, 1234, dfv1.DaemonServicePort).
		TerminateAllPodPortForwards()

	// Check vertex pod metrics endpoints
	HTTPExpect(s.T(), "https://localhost:8001").GET("/metrics").
		Expect().
		Status(200).Body().Contains("total")

	HTTPExpect(s.T(), "https://localhost:8002").GET("/metrics").
		Expect().
		Status(200).Body().Contains("total")

	HTTPExpect(s.T(), "https://localhost:8003").GET("/metrics").
		Expect().
		Status(200).Body().Contains("total")

	// Test daemon service with REST
	HTTPExpect(s.T(), "https://localhost:1234").GET(fmt.Sprintf("/api/v1/pipelines/%s/buffers", pipelineName)).
		Expect().
		Status(200).Body().Contains("buffers")

	HTTPExpect(s.T(), "https://localhost:1234").
		GET(fmt.Sprintf("/api/v1/pipelines/%s/buffers/%s", pipelineName, dfv1.GenerateEdgeBufferNames(Namespace, pipelineName, dfv1.Edge{From: "input", To: "p1"})[0])).
		Expect().
		Status(200).Body().Contains("pipeline")

	HTTPExpect(s.T(), "https://localhost:1234").
		GET(fmt.Sprintf("/api/v1/pipelines/%s/vertices/%s/metrics", pipelineName, "p1")).
		Expect().
		Status(200).Body().Contains("pipeline")

	// Test Daemon service with gRPC
	client, err := daemonclient.NewDaemonServiceClient("localhost:1234")
	assert.NoError(s.T(), err)
	defer func() {
		_ = client.Close()
	}()
	buffers, err := client.ListPipelineBuffers(context.Background(), pipelineName)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), 2, len(buffers))
	bufferInfo, err := client.GetPipelineBuffer(context.Background(), pipelineName, dfv1.GenerateEdgeBufferNames(Namespace, pipelineName, dfv1.Edge{From: "input", To: "p1"})[0])
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), "input", *bufferInfo.FromVertex)
	m, err := client.GetVertexMetrics(context.Background(), pipelineName, "p1")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), pipelineName, *m.Pipeline)
}

func (s *FunctionalSuite) TestFiltering() {
	w := s.Given().Pipeline("@testdata/filtering.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()

	w.Expect().
		VertexPodsRunning().
		VertexPodLogContains("in", LogSourceVertexStarted).
		VertexPodLogContains("p1", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("out", LogSinkVertexStarted)

	defer w.VertexPodPortForward("in", 8443, dfv1.VertexHTTPSPort).
		TerminateAllPodPortForwards()

	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte(`{"id": 180, "msg": "hello", "expect0": "fail", "desc": "A bad example"}`)).
		Expect().
		Status(204)

	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte(`{"id": 80, "msg": "hello1", "expect1": "fail", "desc": "A bad example"}`)).
		Expect().
		Status(204)

	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte(`{"id": 80, "msg": "hello", "expect2": "fail", "desc": "A bad example"}`)).
		Expect().
		Status(204)

	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte(`{"id": 80, "msg": "hello", "expect3": "succeed", "desc": "A good example"}`)).
		Expect().
		Status(204)

	time.Sleep(time.Minute * 1)
	w.Expect().OutputSinkContains("expect3", SinkCheckOptionWithCount(1), SinkCheckOptionWithTimeout(2*time.Second))
	w.Expect().OutputSinkNotContains("expect0", SinkCheckOptionWithTimeout(2*time.Second))
	w.Expect().OutputSinkNotContains("expect1", SinkCheckOptionWithTimeout(2*time.Second))
	w.Expect().OutputSinkNotContains("expect2", SinkCheckOptionWithTimeout(2*time.Second))
}

func (s *FunctionalSuite) TestWatermarkEnabled() {
	w := s.Given().Pipeline("@testdata/watermark.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()

	pipelineName := "simple-pipeline-watermark"
	// TODO: Any way to extract the list from suite
	vertexList := []string{"input", "cat1", "cat2", "cat3", "output1", "output2"}

	w.Expect().
		VertexPodsRunning().DaemonPodsRunning().
		VertexPodLogContains("input", LogSourceVertexStarted).
		VertexPodLogContains("cat1", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("cat2", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("cat3", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("output1", LogSinkVertexStarted).
		VertexPodLogContains("output2", LogSinkVertexStarted).
		DaemonPodLogContains(pipelineName, LogDaemonStarted).
		VertexPodLogContains("output1", `"Data":".*","Createdts":.*`).
		VertexPodLogContains("output2", `"Data":".*","Createdts":.*`)

	defer w.DaemonPodPortForward(pipelineName, 1234, dfv1.DaemonServicePort).
		TerminateAllPodPortForwards()

	// Test Daemon service with gRPC
	client, err := daemonclient.NewDaemonServiceClient("localhost:1234")
	assert.NoError(s.T(), err)
	defer func() {
		_ = client.Close()
	}()
	buffers, err := client.ListPipelineBuffers(context.Background(), pipelineName)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), 5, len(buffers))
	bufferInfo, err := client.GetPipelineBuffer(context.Background(), pipelineName, dfv1.GenerateEdgeBufferNames(Namespace, pipelineName, dfv1.Edge{From: "input", To: "cat1"})[0])
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), "input", *bufferInfo.FromVertex)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for _, vertex := range vertexList {
		wg.Add(1)
		go func(vertex string) {
			defer wg.Done()
			// timeout for checking watermark progression
			isProgressing, err := isWatermarkProgressing(ctx, client, pipelineName, vertex, 3)
			assert.NoError(s.T(), err, "TestWatermarkEnabled failed %s\n", err)
			assert.Truef(s.T(), isProgressing, "isWatermarkProgressing\n")
		}(vertex)
	}
	wg.Wait()
}

// isWatermarkProgressing checks whether the watermark for a given vertex is progressing monotonically.
// progressCount is the number of progressions the watermark value should undertake within the timeout deadline for it
// to be considered as valid.
func isWatermarkProgressing(ctx context.Context, client *daemonclient.DaemonClient, pipelineName string, vertexName string, progressCount int) (bool, error) {
	prevWatermark := int64(-1)
	for i := 0; i < progressCount; i++ {
		currentWatermark := prevWatermark
		for currentWatermark <= prevWatermark {
			wm, err := client.GetVertexWatermark(ctx, pipelineName, vertexName)
			if err != nil {
				return false, err
			}
			currentWatermark = *wm.Watermark
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			default:
				time.Sleep(10 * time.Millisecond)
			}
		}
		prevWatermark = currentWatermark
	}
	return true, nil
}

func TestFunctionalSuite(t *testing.T) {
	suite.Run(t, new(FunctionalSuite))
}
