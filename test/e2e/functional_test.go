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
	"fmt"
	"testing"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	. "github.com/numaproj/numaflow/test/fixtures"
	"github.com/stretchr/testify/suite"
)

//go:generate kubectl -n numaflow-system delete statefulset redis-cluster --ignore-not-found=true
//go:generate kubectl apply -k ../../config/apps/redis -n numaflow-system
// Wait for redis cluster to come up
//go:generate sleep 120

type FunctionalSuite struct {
	E2ESuite
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

	fmt.Print("Start sleeping 1m...")
	time.Sleep(time.Minute * 1)
	w.Expect().OutputSinkContains("out", "expect3", SinkCheckOptionWithCount(1), SinkCheckOptionWithTimeout(2*time.Second))
	w.Expect().OutputSinkNotContains("out", "expect0", SinkCheckOptionWithTimeout(2*time.Second))
	w.Expect().OutputSinkNotContains("out", "expect1", SinkCheckOptionWithTimeout(2*time.Second))
	w.Expect().OutputSinkNotContains("out", "expect2", SinkCheckOptionWithTimeout(2*time.Second))
}

func (s *FunctionalSuite) TestConditionalForwarding() {
	w := s.Given().Pipeline("@testdata/even-odd.yaml").
		When().
		CreatePipelineAndWait()

	defer w.DeletePipelineAndWait()

	w.Expect().
		VertexPodsRunning().
		VertexPodLogContains("in", LogSourceVertexStarted).
		VertexPodLogContains("even-or-odd", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("even-sink", LogSinkVertexStarted).
		VertexPodLogContains("odd-sink", LogSinkVertexStarted).
		VertexPodLogContains("number-sink", LogSinkVertexStarted)
	defer w.VertexPodPortForward("in", 8443, dfv1.VertexHTTPSPort).
		TerminateAllPodPortForwards()

	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("888888")).
		Expect().
		Status(204)
	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("888889")).
		Expect().
		Status(204)
	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("not-an-integer")).
		Expect().
		Status(204)

	time.Sleep(time.Minute * 1)
	// Limitation - The regex string itself has to be REST APU url compatible.
	// Meaning it can't contain special characters like space, star, question mark etc.
	// Otherwise, the E2E will fail with 400 Bad Request Error.
	w.Expect().OutputSinkContains("even-sink", "888888", SinkCheckOptionWithCount(1), SinkCheckOptionWithTimeout(2*time.Second))
	w.Expect().OutputSinkNotContains("even-sink", "888889", SinkCheckOptionWithCount(1), SinkCheckOptionWithTimeout(2*time.Second))
	w.Expect().OutputSinkNotContains("even-sink", "not-an-integer", SinkCheckOptionWithCount(1), SinkCheckOptionWithTimeout(2*time.Second))

	w.Expect().OutputSinkContains("odd-sink", "888889", SinkCheckOptionWithCount(1), SinkCheckOptionWithTimeout(2*time.Second))
	w.Expect().OutputSinkNotContains("odd-sink", "888888", SinkCheckOptionWithCount(1), SinkCheckOptionWithTimeout(2*time.Second))
	w.Expect().OutputSinkNotContains("odd-sink", "not-an-integer", SinkCheckOptionWithCount(1), SinkCheckOptionWithTimeout(2*time.Second))

	w.Expect().OutputSinkContains("number-sink", "888888", SinkCheckOptionWithCount(1), SinkCheckOptionWithTimeout(2*time.Second))
	w.Expect().OutputSinkContains("number-sink", "888889", SinkCheckOptionWithCount(1), SinkCheckOptionWithTimeout(2*time.Second))
	w.Expect().OutputSinkNotContains("number-sink", "not-an-integer", SinkCheckOptionWithCount(1), SinkCheckOptionWithTimeout(2*time.Second))
}

func TestFunctionalSuite(t *testing.T) {
	suite.Run(t, new(FunctionalSuite))
}
