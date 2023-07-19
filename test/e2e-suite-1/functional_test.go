//go:build test

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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	daemonclient "github.com/numaproj/numaflow/pkg/daemon/client"
	. "github.com/numaproj/numaflow/test/fixtures"
)

type FunctionalSuite struct {
	E2ESuite
}

func (s *FunctionalSuite) TestBuiltinEventTimeExtractor() {
	w := s.Given().Pipeline("@testdata/extract-event-time-from-payload.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "extract-event-time"

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning().DaemonPodsRunning()

	defer w.DaemonPodPortForward(pipelineName, 1234, dfv1.DaemonServicePort).
		TerminateAllPodPortForwards()

	// Use daemon client to verify watermark propagation.
	client, err := daemonclient.NewDaemonServiceClient("localhost:1234")
	assert.NoError(s.T(), err)
	defer func() {
		_ = client.Close()
	}()

	// In this test, we send a message with event time being now, apply event time extractor and verify from log that the message event time gets updated.
	testMsgOne := `{"test": 21, "item": [{"id": 1, "name": "numa", "time": "2022-02-18T21:54:42.123Z"},{"id": 2, "name": "numa", "time": "2021-01-18T21:54:42.123Z"}]}`
	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(testMsgOne)))
	w.Expect().VertexPodLogContains("out", fmt.Sprintf("EventTime -  %d", time.Date(2021, 1, 18, 21, 54, 42, 123000000, time.UTC).UnixMilli()), PodLogCheckOptionWithCount(1))

	// Verify watermark is generated based on the new event time.
	testMsgTwo := `{"test": 21, "item": [{"id": 1, "name": "numa", "time": "2022-02-18T21:54:42.123Z"},{"id": 2, "name": "numa", "time": "2021-02-18T21:54:42.123Z"}]}`
	testMsgThree := `{"test": 21, "item": [{"id": 1, "name": "numa", "time": "2022-02-18T21:54:42.123Z"},{"id": 2, "name": "numa", "time": "2021-03-18T21:54:42.123Z"}]}`
	testMsgFour := `{"test": 21, "item": [{"id": 1, "name": "numa", "time": "2022-02-18T21:54:42.123Z"},{"id": 2, "name": "numa", "time": "2021-04-18T21:54:42.123Z"}]}`
	testMsgFive := `{"test": 21, "item": [{"id": 1, "name": "numa", "time": "2022-02-18T21:54:42.123Z"},{"id": 2, "name": "numa", "time": "2021-05-18T21:54:42.123Z"}]}`
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(testMsgTwo)))
	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(testMsgThree)))
	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(testMsgFour)))
	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(testMsgFive)))

wmLoop:
	for {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				s.T().Log("test timed out")
				assert.Fail(s.T(), "timed out")
				break wmLoop
			}
		default:
			wm, err := client.GetPipelineWatermarks(ctx, pipelineName)
			edgeWM := wm[0].Watermarks[0]
			if wm[0].Watermarks[0] != -1 {
				assert.NoError(s.T(), err)
				if err != nil {
					assert.Fail(s.T(), err.Error())
				}
				// Watermark propagation can delay, we consider the test as passed as long as the retrieved watermark matches one of the assigned event times.
				assert.True(s.T(), edgeWM == time.Date(2021, 5, 18, 21, 54, 42, 123000000, time.UTC).UnixMilli() || edgeWM == time.Date(2021, 4, 18, 21, 54, 42, 123000000, time.UTC).UnixMilli() || edgeWM == time.Date(2021, 3, 18, 21, 54, 42, 123000000, time.UTC).UnixMilli() || edgeWM == time.Date(2021, 2, 18, 21, 54, 42, 123000000, time.UTC).UnixMilli() || edgeWM == time.Date(2021, 1, 18, 21, 54, 42, 123000000, time.UTC).UnixMilli())
				break wmLoop
			}
			w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(testMsgFive)))
			time.Sleep(time.Second)
		}
	}
}

func (s *FunctionalSuite) TestWatermarkEnabled() {
	w := s.Given().Pipeline("@testdata/watermark.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()

	pipelineName := "simple-pipeline-watermark"
	// TODO: Any way to extract the list from suite
	edgeList := []string{"input-cat1", "input-cat2", "cat1-output1", "cat2-cat3", "cat3-output2"}

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning().DaemonPodsRunning()

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
	assert.Equal(s.T(), 8, len(buffers))
	bufferInfo, err := client.GetPipelineBuffer(context.Background(), pipelineName, dfv1.GenerateBufferName(Namespace, pipelineName, "cat1", 0))
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), pipelineName, *bufferInfo.Pipeline)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	isProgressing, err := isWatermarkProgressing(ctx, client, pipelineName, edgeList, 3)
	assert.NoError(s.T(), err, "TestWatermarkEnabled failed %s\n", err)
	assert.Truef(s.T(), isProgressing, "isWatermarkProgressing\n")
}

// isWatermarkProgressing checks whether the watermark for each edge in a pipeline is progressing monotonically.
// progressCount is the number of progressions the watermark value should undertake within the timeout deadline for it
func isWatermarkProgressing(ctx context.Context, client *daemonclient.DaemonClient, pipelineName string, edgeList []string, progressCount int) (bool, error) {
	prevWatermark := make([]int64, len(edgeList))
	for i := 0; i < len(edgeList); i++ {
		prevWatermark[i] = -1
	}
	for i := 0; i < progressCount; i++ {
		currentWatermark := prevWatermark
		for func(current []int64, prev []int64) bool {
			for j := 0; j < len(current); j++ {
				if current[j] > prev[j] {
					return false
				}
			}
			return true
		}(currentWatermark, prevWatermark) {
			wm, err := client.GetPipelineWatermarks(ctx, pipelineName)
			if err != nil {
				return false, err
			}
			pipelineWatermarks := make([]int64, len(edgeList))
			idx := 0
			for _, e := range wm {
				pipelineWatermarks[idx] = e.Watermarks[0]
				idx++
			}
			currentWatermark = pipelineWatermarks
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
