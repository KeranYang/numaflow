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

	for i := 1; i <= 100; i++ {
		w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("888888")))
	}

	time.Sleep(time.Minute * 2)

	// All messages should be written to retry-until-success sink.
	w.Expect().SinkContains("retry-until-success", "888888", WithContainCount(100))
	// At least one message should be written to drop-and-ack-latest sink.
	w.Expect().SinkContains("drop-and-ack-latest", "888888", WithContainCount(1))
	// At least one message should be dropped before writing to drop-and-ack-latest sink.
	w.Expect().SinkNotContains("drop-and-ack-latest", "888888", WithContainCount(100))
}

func TestFunctionalSuite(t *testing.T) {
	suite.Run(t, new(FunctionalSuite))
}
