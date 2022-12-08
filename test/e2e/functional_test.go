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
	"testing"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	. "github.com/numaproj/numaflow/test/fixtures"
	"github.com/stretchr/testify/suite"
)

type FunctionalSuite struct {
	E2ESuite
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

	println("KeranTest")
	println(w.GetOnePodIp("in"))

	w.SendMessageTo("even-odd", "in", "888888")

	// HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("888888")).
	//	Expect().
	//	Status(204)

	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("888889")).
		Expect().
		Status(204)
	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("not an integer")).
		Expect().
		Status(204)

	// time.Sleep(time.Minute * 5)
	w.Expect().VertexPodLogContains("even-sink", "888888")
	w.Expect().VertexPodLogNotContains("even-sink", "888889", PodLogCheckOptionWithTimeout(2*time.Second))
	w.Expect().VertexPodLogNotContains("even-sink", "not an integer", PodLogCheckOptionWithTimeout(2*time.Second))

	w.Expect().VertexPodLogContains("odd-sink", "888889")
	w.Expect().VertexPodLogNotContains("odd-sink", "888888", PodLogCheckOptionWithTimeout(2*time.Second))
	w.Expect().VertexPodLogNotContains("odd-sink", "not an integer", PodLogCheckOptionWithTimeout(2*time.Second))

	w.Expect().VertexPodLogContains("number-sink", "888888")
	w.Expect().VertexPodLogContains("number-sink", "888889")
	w.Expect().VertexPodLogNotContains("number-sink", "not an integer", PodLogCheckOptionWithTimeout(2*time.Second))
}

func TestFunctionalSuite(t *testing.T) {
	suite.Run(t, new(FunctionalSuite))
}
