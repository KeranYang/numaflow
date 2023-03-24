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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	. "github.com/numaproj/numaflow/test/fixtures"
)

type FunctionalSuite struct {
	E2ESuite
}

func (s *FunctionalSuite) TestDropOnFull() {
	w := s.Given().Pipeline("@testdata/drop-on-full.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "drop-on-full"

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	// Scale the sinks down to 0 pod to create a buffer full scenario.
	scaleDownArgs := "kubectl scale vtx drop-on-full-drop-sink --replicas=0 -n numaflow-system"
	w.Exec("/bin/sh", []string{"-c", scaleDownArgs}, CheckVertexScaled)
	scaleDownArgs = "kubectl scale vtx drop-on-full-retry-sink --replicas=0 -n numaflow-system"
	w.Exec("/bin/sh", []string{"-c", scaleDownArgs}, CheckVertexScaled)

	for i := 1; i <= 2; i++ {
		w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(strconv.Itoa(i))))
		// Give buffer writer 2 seconds to update it's isFull attribute.
		time.Sleep(time.Second * 2)
	}

	// Scale sink up to 1 pod to process the message from the buffer.
	scaleUpArgs := "kubectl scale vtx drop-on-full-drop-sink --replicas=1 -n numaflow-system"
	w.Exec("/bin/sh", []string{"-c", scaleUpArgs}, CheckVertexScaled)
	scaleUpArgs = "kubectl scale vtx drop-on-full-retry-sink --replicas=1 -n numaflow-system"
	w.Exec("/bin/sh", []string{"-c", scaleUpArgs}, CheckVertexScaled)
	w.Expect().VertexPodsRunning()

	w.Expect().SinkContains("retry-sink", "1")
	w.Expect().SinkContains("retry-sink", "2")
	w.Expect().SinkContains("drop-sink", "1")
	w.Expect().SinkNotContains("drop-sink", "2")
}

func TestFunctionalSuite(t *testing.T) {
	suite.Run(t, new(FunctionalSuite))
}
