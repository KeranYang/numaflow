package publish

import (
	"context"
	"fmt"
	"time"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/watermark/entity"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

// SourcePublisher publishes source watermarks based on a list of isb.ReadMessage
// and also publishes idle watermarks.
type SourcePublisher interface {
	// PublishSourceWatermarks publishes source watermarks.
	PublishSourceWatermarks([]*isb.ReadMessage)
	// PublishIdleWatermarks publishes idle watermarks.
	PublishIdleWatermarks(time.Time)
}

type sourcePublisher struct {
	ctx                context.Context
	pipelineName       string
	vertexName         string
	srcPublishWMStores store.WatermarkStore
	sourcePublishWMs   map[int32]Publisher
	opts               *publishOptions
}

// NewSourcePublisher returns a new source publisher.
func NewSourcePublisher(ctx context.Context, pipelineName, vertexName string, srcPublishWMStores store.WatermarkStore, opts ...PublishOption) SourcePublisher {
	options := &publishOptions{}
	for _, opt := range opts {
		opt(options)
	}
	return &sourcePublisher{
		ctx:                ctx,
		pipelineName:       pipelineName,
		vertexName:         vertexName,
		srcPublishWMStores: srcPublishWMStores,
		sourcePublishWMs:   make(map[int32]Publisher),
		opts:               options,
	}
}

// PublishSourceWatermarks publishes source watermarks for a list of isb.ReadMessage.
// it publishes for the partitions to which the messages belong, it publishes the oldest timestamp
// seen for that partition in the list of messages.
// if a publisher for a partition doesn't exist, it creates one.
func (df *sourcePublisher) PublishSourceWatermarks(readMessages []*isb.ReadMessage) {
	// oldestTimestamps stores the latest timestamps for different partitions
	oldestTimestamps := make(map[int32]time.Time)
	for _, m := range readMessages {
		// Get latest timestamps for different partitions
		if t, ok := oldestTimestamps[m.ReadOffset.PartitionIdx()]; !ok || m.EventTime.Before(t) {
			oldestTimestamps[m.ReadOffset.PartitionIdx()] = m.EventTime
		}
	}
	for p, t := range oldestTimestamps {
		publisher := df.loadSourceWatermarkPublisher(p)
		// toVertexPartitionIdx is 0 because we publish watermarks within the source itself.
		publisher.PublishWatermark(wmb.Watermark(t), nil, 0) // we don't care about the offset while publishing source watermark
	}
}

// PublishIdleWatermarks publishes idle watermarks for all partitions.
func (df *sourcePublisher) PublishIdleWatermarks(wm time.Time) {
	for partitionId, publisher := range df.sourcePublishWMs {
		publisher.PublishIdleWatermark(wmb.Watermark(wm), nil, partitionId) // while publishing idle watermark at source, we don't care about the offset
	}
}

// loadSourceWatermarkPublisher does a lazy load on the watermark publisher
func (df *sourcePublisher) loadSourceWatermarkPublisher(partitionID int32) Publisher {
	if p, ok := df.sourcePublishWMs[partitionID]; ok {
		return p
	}
	entityName := fmt.Sprintf("%s-%s-%d", df.pipelineName, df.vertexName, partitionID)
	processorEntity := entity.NewProcessorEntity(entityName)
	// toVertexPartitionCount is 1 because we publish watermarks within the source itself.
	sourcePublishWM := NewPublish(df.ctx, processorEntity, df.srcPublishWMStores, 1, IsSource(), WithDelay(df.opts.delay))
	df.sourcePublishWMs[partitionID] = sourcePublishWM
	return sourcePublishWM
}
