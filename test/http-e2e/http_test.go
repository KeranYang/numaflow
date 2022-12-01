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

package http_e2e

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/nats-io/nats.go"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/jetstream"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	. "github.com/numaproj/numaflow/test/fixtures"
	"github.com/stretchr/testify/suite"
)

//go:generate kubectl apply -f testdata/http-auth-fake-secret.yaml -n numaflow-system

type HTTPSuite struct {
	E2ESuite
}

func (s *HTTPSuite) TestHTTPSourcePipeline() {
	// connect to NATS
	nc, err := jsclient.NewDefaultJetStreamClient(nats.DefaultURL).Connect(context.TODO())
	assert.NoError(s.T(), err)
	defer nc.Close()

	// create JetStream Context
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
	assert.NoError(s.T(), err)

	// create test bucket
	_, err = js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:       "keran-test-bucket",
		Description:  "keran-test-bucket",
		MaxValueSize: 0,
		History:      0,
		TTL:          0,
		MaxBytes:     0,
		Storage:      nats.MemoryStorage,
		Replicas:     0,
		Placement:    nil,
	})

	defer func() { _ = js.DeleteKeyValue("keran-test-bucket") }()
	assert.NoError(s.T(), err)

	// for JetStream KeyValue store, the bucket should have been created in advance
	kv, err := js.KeyValue("keran-test-bucket")
	assert.NoError(s.T(), err)

	w := s.Given().Pipeline("@testdata/http-source.yaml").
		When().
		CreatePipelineAndWait()

	defer w.DeletePipelineAndWait()

	w.Expect().
		VertexPodsRunning().
		VertexPodLogContains("in", LogSourceVertexStarted).
		VertexPodLogContains("out", LogSinkVertexStarted)

	defer w.VertexPodPortForward("in", 8443, dfv1.VertexHTTPSPort).
		TerminateAllPodPortForwards()

	// Check Service
	cmd := fmt.Sprintf("kubectl -n %s get svc -lnumaflow.numaproj.io/pipeline-name=%s,numaflow.numaproj.io/vertex-name=%s | grep -v CLUSTER-IP | grep -v headless", Namespace, "http-source", "in")
	w.Exec("sh", []string{"-c", cmd}, OutputRegexp("http-source-in"))

	HTTPExpect(s.T(), "https://localhost:8443").GET("/health").Expect().Status(204)

	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("no-id")).
		Expect().
		Status(204)
	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("no-id")).
		Expect().
		Status(204)
	// No x-numaflow-id, expect 2 outputs
	// w.Expect().VertexPodLogContains("out", "no-id", PodLogCheckOptionWithCount(2))

	time.Sleep(time.Second * 30)

	entry, err := kv.Get(string([]byte("no-id")))
	if err != nil {
		fmt.Printf("keran-test, kv get error %v", err)
		println(err)
	}
	assert.Equal(s.T(), int64(2), int64(binary.LittleEndian.Uint64(entry.Value())))

	w.Expect().VertexPodLogContains("out", "no-id", PodLogCheckOptionWithCount(2))

	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithHeader("x-numaflow-id", "101").WithBytes([]byte("with-id")).
		Expect().
		Status(204)
	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithHeader("x-numaflow-id", "101").WithBytes([]byte("with-id")).
		Expect().
		Status(204)
	// With same x-numaflow-id, expect 1 output
	// w.Expect().VertexPodLogContains("out", "with-id", PodLogCheckOptionWithCount(1))
	time.Sleep(time.Second * 30)

	entry, err = kv.Get("with-id")
	assert.Equal(s.T(), int64(1), int64(binary.LittleEndian.Uint64(entry.Value())))

	w.Expect().VertexPodLogContains("out", "with-id", PodLogCheckOptionWithCount(1))

	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithHeader("x-numaflow-id", "102").WithBytes([]byte("with-id")).
		Expect().
		Status(204)
	// With a new x-numaflow-id, expect 2 outputs
	// w.Expect().VertexPodLogContains("out", "with-id", PodLogCheckOptionWithCount(2))
	time.Sleep(time.Second * 30)

	entry, err = kv.Get("with-id")
	assert.Equal(s.T(), int64(2), int64(binary.LittleEndian.Uint64(entry.Value())))

	w.Expect().VertexPodLogContains("out", "with-id", PodLogCheckOptionWithCount(2))
}

func (s *HTTPSuite) TestHTTPSourceAuthPipeline() {
	w := s.Given().Pipeline("@testdata/http-source-with-auth.yaml").
		When().
		CreatePipelineAndWait()

	defer w.DeletePipelineAndWait()

	w.Expect().
		VertexPodsRunning().
		VertexPodLogContains("in", LogSourceVertexStarted).
		VertexPodLogContains("out", LogSinkVertexStarted)

	defer w.VertexPodPortForward("in", 8443, dfv1.VertexHTTPSPort).
		TerminateAllPodPortForwards()

	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("no-auth")).
		Expect().
		Status(403)

	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithHeader("Authorization", "Bearer faketoken").WithBytes([]byte("with-auth")).
		Expect().
		Status(204)

	w.Expect().VertexPodLogContains("out", "with-auth")
}

func TestHTTPSuite(t *testing.T) {
	suite.Run(t, new(HTTPSuite))
}
