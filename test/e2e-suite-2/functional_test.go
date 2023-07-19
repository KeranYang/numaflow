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
	"strings"
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

func (s *FunctionalSuite) TestSourceFiltering() {
	w := s.Given().Pipeline("@testdata/source-filtering.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "source-filtering"

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	expect0 := `{"id": 180, "msg": "hello", "expect0": "fail", "desc": "A bad example"}`
	expect1 := `{"id": 80, "msg": "hello1", "expect1": "fail", "desc": "A bad example"}`
	expect2 := `{"id": 80, "msg": "hello", "expect2": "fail", "desc": "A bad example"}`
	expect3 := `{"id": 80, "msg": "hello", "expect3": "succeed", "desc": "A good example"}`
	expect4 := `{"id": 80, "msg": "hello", "expect4": "succeed", "desc": "A good example"}`

	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(expect0))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(expect1))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(expect2))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(expect3))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(expect4)))

	w.Expect().SinkContains("out", expect3)
	w.Expect().SinkContains("out", expect4)
	w.Expect().SinkNotContains("out", expect0)
	w.Expect().SinkNotContains("out", expect1)
	w.Expect().SinkNotContains("out", expect2)
}

func (s *FunctionalSuite) TestDropOnFull() {
	w := s.Given().Pipeline("@testdata/drop-on-full.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "drop-on-full"

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()
	defer w.VertexPodPortForward("in", 8001, dfv1.VertexMetricsPort).
		TerminateAllPodPortForwards()

	// scale the sinks down to 0 pod to create a buffer full scenario.
	scaleDownArgs := "kubectl scale vtx drop-on-full-sink --replicas=0 -n numaflow-system"
	w.Exec("/bin/sh", []string{"-c", scaleDownArgs}, CheckVertexScaled)
	w.Expect().VertexSizeScaledTo("sink", 0)

	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("1")))
	// give buffer writer some time to update the isFull attribute.
	// 5s is a carefully chosen number to create a stable buffer full scenario.
	time.Sleep(time.Second * 5)
	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("2")))

	expectedDropMetric := `forwarder_drop_total{partition_name="numaflow-system-drop-on-full-sink-0",pipeline="drop-on-full",vertex="in"} 1`
	// wait for the drop metric to be updated, time out after 10s.
	timeoutChan := time.After(time.Second * 10)
	ticker := time.NewTicker(time.Second * 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			metricsString := HTTPExpect(s.T(), "https://localhost:8001").GET("/metrics").
				Expect().
				Status(200).Body().Raw()
			if strings.Contains(metricsString, expectedDropMetric) {
				return
			}
		case <-timeoutChan:
			s.T().Fatalf("timeout waiting for metrics to be updated")
		}
	}
}

func (s *FunctionalSuite) TestTimeExtractionFilter() {
	w := s.Given().Pipeline("@testdata/time-extraction-filter.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "time-extraction-filter"

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	testMsgOne := `{"id": 80, "msg": "hello", "time": "2021-01-18T21:54:42.123Z", "desc": "A good ID."}`
	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(testMsgOne)))
	w.Expect().VertexPodLogContains("out", fmt.Sprintf("EventTime -  %d", time.Date(2021, 1, 18, 21, 54, 42, 123000000, time.UTC).UnixMilli()), PodLogCheckOptionWithCount(1), PodLogCheckOptionWithContainer("numa"))

	testMsgTwo := `{"id": 101, "msg": "test", "time": "2021-01-18T21:54:42.123Z", "desc": "A bad ID."}`
	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(testMsgTwo)))
	w.Expect().SinkNotContains("out", testMsgTwo)
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
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
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
			s.T().Log("keep waiting for watermark to be generated")
		}
	}
}

func TestFunctionalSuite(t *testing.T) {
	suite.Run(t, new(FunctionalSuite))
}
