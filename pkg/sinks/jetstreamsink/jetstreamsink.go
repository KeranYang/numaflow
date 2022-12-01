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

package jetstreamsink

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/udf/applier"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"strconv"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/forward"
	metricspkg "github.com/numaproj/numaflow/pkg/metrics"
)

// JetStreamSink is a sink to publish to JetStream
type JetStreamSink struct {
	name         string
	pipelineName string
	isdf         *forward.InterStepDataForward
	logger       *zap.SugaredLogger
}

type Option func(sink *JetStreamSink) error

func WithLogger(log *zap.SugaredLogger) Option {
	return func(jss *JetStreamSink) error {
		jss.logger = log
		return nil
	}
}

// NewJetStreamSink returns JetStreamSink type.
func NewJetStreamSink(vertex *dfv1.Vertex, fromBuffer isb.BufferReader, fetchWatermark fetch.Fetcher, publishWatermark map[string]publish.Publisher, opts ...Option) (*JetStreamSink, error) {
	bh := new(JetStreamSink)
	name := vertex.Spec.Name
	bh.name = name
	bh.pipelineName = vertex.Spec.PipelineName

	for _, o := range opts {
		if err := o(bh); err != nil {
			return nil, err
		}
	}
	if bh.logger == nil {
		bh.logger = logging.NewLogger()
	}

	forwardOpts := []forward.Option{forward.WithVertexType(dfv1.VertexTypeSink), forward.WithLogger(bh.logger)}
	if x := vertex.Spec.Limits; x != nil {
		if x.ReadBatchSize != nil {
			forwardOpts = append(forwardOpts, forward.WithReadBatchSize(int64(*x.ReadBatchSize)))
		}
	}

	isdf, err := forward.NewInterStepDataForward(vertex, fromBuffer, map[string]isb.BufferWriter{vertex.GetToBuffers()[0].Name: bh}, forward.All, applier.Terminal, fetchWatermark, publishWatermark, forwardOpts...)
	if err != nil {
		return nil, err
	}
	bh.isdf = isdf

	return bh, nil
}

// GetName returns the name.
func (jss *JetStreamSink) GetName() string {
	return jss.name
}

// IsFull returns whether sink is full, which is never true.
func (jss *JetStreamSink) IsFull() bool {
	// printing can never be full
	return false
}

// Write writes to the jetstream sink.
func (jss *JetStreamSink) Write(context context.Context, messages []isb.Message) ([]isb.Offset, []error) {
	client := redis.NewClient(&redis.Options{
		Addr:     "redis-cluster:6379",
		Password: "",
		DB:       0,
	})

	for _, msg := range messages {
		key := string(msg.Payload)
		entry, err := client.Get(context, key).Result()
		if err != nil {
			client.Set(context, key, 1, 5*time.Minute)
		} else {
			count, err := strconv.Atoi(entry)
			if err != nil {
				fmt.Printf("Atoi converting error %v", err)
			}
			client.Set(context, key, count+1, 5*time.Minute)
		}
	}

	sinkWriteCount.With(map[string]string{metricspkg.LabelVertex: jss.name, metricspkg.LabelPipeline: jss.pipelineName}).Add(float64(len(messages)))

	return nil, make([]error, len(messages))
}

func (jss *JetStreamSink) Close() error {
	return nil
}

// Start starts the sink.
func (jss *JetStreamSink) Start() <-chan struct{} {
	return jss.isdf.Start()
}

// Stop stops sinking
func (jss *JetStreamSink) Stop() {
	jss.isdf.Stop()
}

// ForceStop stops sinking
func (jss *JetStreamSink) ForceStop() {
	jss.isdf.ForceStop()
}
