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
	"github.com/stretchr/testify/suite"
	"testing"

	. "github.com/numaproj/numaflow/test/fixtures"
)

type FunctionalSuite struct {
	E2ESuite
}

func (s *FunctionalSuite) TestSourceDataTransform() {
	w := s.Given().Pipeline("@testdata/source-data-transform.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "source-data-transform"

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("88"))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("89"))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("not an integer")))

	w.Expect().SinkContains("even-sink", "88")
	w.Expect().SinkContains("even-sink", "89")
	w.Expect().SinkContains("even-sink", "not an integer")
}

func TestFunctionalSuite(t *testing.T) {
	suite.Run(t, new(FunctionalSuite))
}
