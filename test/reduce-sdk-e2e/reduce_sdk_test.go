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

package reduce_sdk_e2e

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	. "github.com/numaproj/numaflow/test/fixtures"
)

type ReduceSDKSuite struct {
	E2ESuite
}

func (s *ReduceSDKSuite) TestReduceStreamGo() {
	s.testReduceStream("go")
}

func (s *ReduceSDKSuite) TestReduceStreamJava() {
	s.testReduceStream("java")
}

func (s *ReduceSDKSuite) testReduceStream(lang string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	w := s.Given().Pipeline(fmt.Sprintf("@testdata/reduce-stream/reduce-stream-%s.yaml", lang)).
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := fmt.Sprintf("reduce-stream-%s", lang)

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	done := make(chan struct{})
	go func() {
		// publish messages to source vertex, with event time starting from 60000
		startTime := 60000
		for i := 0; true; i++ {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			default:
				eventTime := strconv.Itoa(startTime + i*1000)
				w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("3")).WithHeader("X-Numaflow-Event-Time", eventTime))
			}
		}
	}()

	// The reduce stream application summarizes the input messages and returns the sum when the sum is greater than 100.
	// Since we are sending 3s, the first returned message should be 102.
	// There should be no other values.
	w.Expect().SinkContains("sink", "102")
	w.Expect().SinkNotContains("sink", "99")
	w.Expect().SinkNotContains("sink", "105")
	done <- struct{}{}
}

func (s *ReduceSDKSuite) TestReduceSessionJava() {
	s.testReduceSession("java")
}

func (s *ReduceSDKSuite) testReduceSession(lang string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	w := s.Given().Pipeline(fmt.Sprintf("@testdata/reduce-session/reduce-session-%s.yaml", lang)).
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := fmt.Sprintf("reduce-session-%s", lang)

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	count := 0
	done := make(chan struct{})
	go func() {
		// publish messages to source vertex, with event time starting from 60000
		startTime := 60000
		for i := 0; true; i++ {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			default:
				if count == 50 {
					startTime = startTime + (50 * 1000)
					count = 0
				} else {
					startTime = startTime + 1000
				}
				eventTime := strconv.Itoa(startTime)
				w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("1")).WithHeader("X-Numaflow-Event-Time", eventTime))
				count += 1
			}
		}
	}()

	w.Expect().SinkContains("sink", "50")
	done <- struct{}{}
}

func TestSessionSuite(t *testing.T) {
	suite.Run(t, new(ReduceSDKSuite))
}
