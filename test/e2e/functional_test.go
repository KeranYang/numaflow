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

//go:generate kubectl -n numaflow-system delete statefulset zookeeper kafka-broker --ignore-not-found=true
//go:generate kubectl -n numaflow-system delete statefulset redis-cluster --ignore-not-found=true
//go:generate kubectl apply -k ../../config/apps/kafka -n numaflow-system
// Wait for zookeeper to come up
//go:generate sleep 60

type FunctionalSuite struct {
	E2ESuite
}

func (s *FunctionalSuite) TestFilteringRedis() {
	w := s.Given().Pipeline("@testdata/filtering-redis.yaml").
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
	w.Expect().OutputSinkContains("*expect3*", SinkCheckOptionWithCount(1), SinkCheckOptionWithTimeout(2*time.Second))
	w.Expect().OutputSinkNotContains("*expect[0-2]*", SinkCheckOptionWithTimeout(2*time.Second))
}

func TestFunctionalSuite(t *testing.T) {
	suite.Run(t, new(FunctionalSuite))
}
