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

/*
Package forward does the Read (fromBufferPartition) -> Process (map UDF) -> Forward (toBuffers) -> Ack (fromBufferPartition) loop.
*/
package forward

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/util/wait"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/idlehandler"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

// InterStepDataForward forwards the data from previous step to the current step via inter-step buffer.
type InterStepDataForward struct {
	// I have my reasons for overriding the default principle https://github.com/golang/go/issues/22602
	ctx context.Context
	// cancelFn cancels our new context, our cancellation is little more complex and needs to be well orchestrated, hence
	// we need something more than a cancel().
	cancelFn            context.CancelFunc
	fromBufferPartition isb.BufferReader
	// toBuffers is a map of toVertex name to the toVertex's owned buffers.
	toBuffers map[string][]isb.BufferWriter
	fsd       forwarder.ToWhichStepDecider
	wmFetcher fetch.Fetcher
	// wmPublishers stores the vertex to publisher mapping
	wmPublishers  map[string]publish.Publisher
	opts          options
	vertexName    string
	pipelineName  string
	vertexReplica int32
	// idleManager manages the idle watermark status.
	idleManager wmb.IdleManager
	// wmbChecker checks if the idle watermark is valid when the len(readMessage) is 0.
	wmbChecker wmb.WMBChecker
	Shutdown
}

// NewInterStepDataForward creates an inter-step forwarder.
func NewInterStepDataForward(vertexInstance *dfv1.VertexInstance, fromStep isb.BufferReader, toSteps map[string][]isb.BufferWriter, fsd forwarder.ToWhichStepDecider, fetchWatermark fetch.Fetcher, publishWatermark map[string]publish.Publisher, idleManager wmb.IdleManager, opts ...Option) (*InterStepDataForward, error) {

	options := DefaultOptions()
	for _, o := range opts {
		if err := o(options); err != nil {
			return nil, err
		}
	}

	// creating a context here which is managed by the forwarder's lifecycle
	ctx, cancel := context.WithCancel(context.Background())

	var isdf = InterStepDataForward{
		ctx:                 ctx,
		cancelFn:            cancel,
		fromBufferPartition: fromStep,
		toBuffers:           toSteps,
		fsd:                 fsd,
		wmFetcher:           fetchWatermark,
		wmPublishers:        publishWatermark,
		// should we do a check here for the values not being null?
		vertexName:    vertexInstance.Vertex.Spec.Name,
		pipelineName:  vertexInstance.Vertex.Spec.PipelineName,
		vertexReplica: vertexInstance.Replica,
		idleManager:   idleManager,
		wmbChecker:    wmb.NewWMBChecker(2), // TODO: make configurable
		Shutdown: Shutdown{
			rwlock: new(sync.RWMutex),
		},
		opts: *options,
	}

	// Add logger from parent ctx to child context.
	isdf.ctx = logging.WithLogger(ctx, options.logger)

	if (isdf.opts.streamMapUdfApplier != nil) && isdf.opts.readBatchSize != 1 {
		return nil, fmt.Errorf("batch size is not 1 with map UDF streaming")
	}

	return &isdf, nil
}

// Start starts reading the buffer and forwards to the next buffers. Call `Stop` to stop.
func (isdf *InterStepDataForward) Start() <-chan error {
	log := logging.FromContext(isdf.ctx)
	stopped := make(chan error)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		log.Info("Starting forwarder...")
		// with wg approach can do more cleanup in case we need in the future.
		defer wg.Done()
		for {
			select {
			case <-isdf.ctx.Done():
				ok, err := isdf.IsShuttingDown()
				if err != nil {
					// ignore the error for now.
					log.Errorw("Failed to check if it can shutdown", zap.Error(err))
				}
				if ok {
					log.Info("Shutting down...")
					return
				}
			default:
				// once context.Done() is called, we still have to try to forwardAChunk because in graceful
				// shutdown the fromBufferPartition should be empty.
			}
			// keep doing what you are good at, if we get an error we will stop.
			if err := isdf.forwardAChunk(isdf.ctx); err != nil {
				log.Errorw("Failed to forward a chunk", zap.Error(err))
				stopped <- err
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		// Clean up resources for buffer reader and all the writers if any.
		if err := isdf.fromBufferPartition.Close(); err != nil {
			log.Errorw("Failed to close buffer reader, shutdown anyways...", zap.Error(err))
		} else {
			log.Infow("Closed buffer reader", zap.String("bufferFrom", isdf.fromBufferPartition.GetName()))
		}
		for _, buffer := range isdf.toBuffers {
			for _, partition := range buffer {
				if err := partition.Close(); err != nil {
					log.Errorw("Failed to close partition writer, shutdown anyways...", zap.Error(err), zap.String("bufferTo", partition.GetName()))
				} else {
					log.Infow("Closed partition writer", zap.String("bufferTo", partition.GetName()))
				}
			}
		}

		close(stopped)
	}()

	return stopped
}

// forwardAChunk forwards a chunk of message from the fromBufferPartition to the toBuffers. It does the Read -> Process -> Forward -> Ack chain
// for a chunk of messages returned by the first Read call. It will return only if only we are successfully able to ack
// the message after forwarding, barring any platform errors. The platform errors include buffer-full,
// buffer-not-reachable, etc., but does not include errors due to user code UDFs, WhereTo, etc.
func (isdf *InterStepDataForward) forwardAChunk(ctx context.Context) error {
	start := time.Now()
	totalBytes := 0
	dataBytes := 0
	// Initialize metric labels
	metricLabels := map[string]string{
		metrics.LabelVertex:             isdf.vertexName,
		metrics.LabelPipeline:           isdf.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeMapUDF),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)),
	}
	metricLabelsWithPartition := map[string]string{
		metrics.LabelVertex:             isdf.vertexName,
		metrics.LabelPipeline:           isdf.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeMapUDF),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)),
		metrics.LabelPartitionName:      isdf.fromBufferPartition.GetName(),
	}
	// There is a chance that we have read the message and the container got forcefully terminated before processing. To provide
	// at-least-once semantics for reading, during restart we will have to reprocess all unacknowledged messages. It is the
	// responsibility of the Read function to do that.
	readStart := time.Now()
	readMessages, err := isdf.fromBufferPartition.Read(ctx, isdf.opts.readBatchSize)
	isdf.opts.logger.Debugw("Read from buffer", zap.String("bufferFrom", isdf.fromBufferPartition.GetName()), zap.Int64("length", int64(len(readMessages))))
	if err != nil {
		isdf.opts.logger.Warnw("failed to read fromBufferPartition", zap.Error(err))
		metrics.ReadMessagesError.With(metricLabelsWithPartition).Inc()
	}

	// process only if we have any read messages. There is a natural looping here if there is an internal error while
	// reading, and we are not able to proceed.
	if len(readMessages) == 0 {
		// When the read length is zero, the write length is definitely zero too,
		// meaning there's no data to be published to the next vertex, and we consider this
		// situation as idling.
		// In order to continue propagating watermark, we will set watermark idle=true and publish it.
		// We also publish a control message if this is the first time we get this idle situation.
		// We compute the HeadIdleWMB using the given partition as the idle watermark
		var processorWMB = isdf.wmFetcher.ComputeHeadIdleWMB(isdf.fromBufferPartition.GetPartitionIdx())
		if !isdf.wmbChecker.ValidateHeadWMB(processorWMB) {
			// validation failed, skip publishing
			isdf.opts.logger.Debugw("skip publishing idle watermark",
				zap.Int("counter", isdf.wmbChecker.GetCounter()),
				zap.Int64("offset", processorWMB.Offset),
				zap.Int64("watermark", processorWMB.Watermark),
				zap.Bool("idle", processorWMB.Idle))
			return nil
		}

		// if the validation passed, we will publish the watermark to all the toBuffer partitions.
		for toVertexName, toVertexBuffer := range isdf.toBuffers {
			for _, partition := range toVertexBuffer {
				if p, ok := isdf.wmPublishers[toVertexName]; ok {
					idlehandler.PublishIdleWatermark(ctx, isdf.fromBufferPartition.GetPartitionIdx(), partition, p, isdf.idleManager, isdf.opts.logger, isdf.vertexName, isdf.pipelineName, dfv1.VertexTypeMapUDF, isdf.vertexReplica, wmb.Watermark(time.UnixMilli(processorWMB.Watermark)))
				}
			}
		}
		return nil
	}

	var dataMessages = make([]*isb.ReadMessage, 0, len(readMessages))

	// store the offsets of the messages we read from ISB
	var readOffsets = make([]isb.Offset, len(readMessages))
	for idx, m := range readMessages {
		readOffsets[idx] = m.ReadOffset
		totalBytes += len(m.Payload)
		if m.Kind == isb.Data {
			dataMessages = append(dataMessages, m)
			dataBytes += len(m.Payload)
		}
	}

	// If we don't have any data messages(we received only wmbs), we can ack all the readOffsets and return early.
	if len(dataMessages) == 0 {
		if err := isdf.ackFromBuffer(ctx, readOffsets); err != nil {
			isdf.opts.logger.Errorw("Failed to ack from buffer", zap.Error(err))
			metrics.AckMessageError.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelVertexType: string(dfv1.VertexTypeMapUDF), metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)), metrics.LabelPartitionName: isdf.fromBufferPartition.GetName()}).Add(float64(len(readOffsets)))
			return err
		}
		return nil
	}

	metrics.ReadProcessingTime.With(metricLabelsWithPartition).Observe(float64(time.Since(readStart).Microseconds()))
	metrics.ReadDataMessagesCount.With(metricLabelsWithPartition).Add(float64(len(dataMessages)))
	metrics.ReadMessagesCount.With(metricLabelsWithPartition).Add(float64(len(readMessages)))
	metrics.ReadBytesCount.With(metricLabelsWithPartition).Add(float64(totalBytes))
	metrics.ReadDataBytesCount.With(metricLabelsWithPartition).Add(float64(dataBytes))

	// fetch watermark if available
	// TODO: make it async (concurrent and wait later)
	// let's track only the first element's watermark. This is important because we reassign the watermark we fetch
	// to all the elements in the batch. If we were to assign last element's watermark, we will wrongly mark on-time data as late.
	// we fetch the watermark for the partition from which we read the message.
	processorWM := isdf.wmFetcher.ComputeWatermark(readMessages[0].ReadOffset, isdf.fromBufferPartition.GetPartitionIdx())

	// assign watermark to data messages
	for _, msg := range dataMessages {
		msg.Watermark = time.Time(processorWM)
		// emit message size metric
	}

	var udfResults []isb.ReadWriteMessagePair
	var writeOffsets map[string][][]isb.Offset
	// Check if map streaming mode is enabled, if the applier is not nil that means we have enabled the required mode
	if isdf.opts.streamMapUdfApplier != nil {
		writeOffsets, err = isdf.streamMessage(ctx, dataMessages)
		if err != nil {
			isdf.opts.logger.Errorw("failed to streamMessage", zap.Error(err))
			// As there's no partial failure, non-ack all the readOffsets
			isdf.fromBufferPartition.NoAck(ctx, readOffsets)
			return err
		}
	} else {
		// create space for writeMessages specific to each step as we could forward to all the steps too.
		var messageToStep = make(map[string][][]isb.Message)
		for toVertex := range isdf.toBuffers {
			// over allocating to have a predictable pattern
			messageToStep[toVertex] = make([][]isb.Message, len(isdf.toBuffers[toVertex]))
		}
		// Trigger the UDF processing based on the mode enabled for map
		// ie Batch Map or unary map
		// This will be a blocking call until the all the UDF results for the batch are received.
		udfStart := time.Now()
		udfResults, err = isdf.applyUDF(ctx, dataMessages)
		if err != nil {
			isdf.opts.logger.Errorw("failed to applyUDF", zap.Error(err))
			// As there's no partial failure, non-ack all the readOffsets
			isdf.fromBufferPartition.NoAck(ctx, readOffsets)
			return err
		}
		metrics.UDFProcessingTime.With(metricLabels).Observe(float64(time.Since(udfStart).Microseconds()))

		// let's figure out which vertex to send the results to.
		// update the toBuffer(s) with writeMessages.
		for _, m := range udfResults {
			// update toBuffers
			for _, message := range m.WriteMessages {
				if err := isdf.whereToStep(message, messageToStep, m.ReadMessage); err != nil {
					isdf.opts.logger.Errorw("failed in whereToStep", zap.Error(err))
					isdf.fromBufferPartition.NoAck(ctx, readOffsets)
					return err
				}
			}
		}

		// forward the message to the edge buffer (could be multiple edges)
		writeOffsets, err = isdf.writeToBuffers(ctx, messageToStep)
		if err != nil {
			isdf.opts.logger.Errorw("failed to write to toBuffers", zap.Error(err))
			isdf.fromBufferPartition.NoAck(ctx, readOffsets)
			return err
		}
		isdf.opts.logger.Debugw("writeToBuffers completed")
	}

	// activeWatermarkBuffers records the buffers that the publisher has published
	// a watermark in this batch processing cycle.
	// it's used to determine which buffers should receive an idle watermark.
	// It is created as a slice because it tracks per partition activity info.
	var activeWatermarkBuffers = make(map[string][]bool)
	// forward the highest watermark to all the edges to avoid idle edge problem
	// TODO: sort and get the highest value
	for toVertexName, toVertexBufferOffsets := range writeOffsets {
		activeWatermarkBuffers[toVertexName] = make([]bool, len(toVertexBufferOffsets))
		if publisher, ok := isdf.wmPublishers[toVertexName]; ok {
			for index, offsets := range toVertexBufferOffsets {
				if len(offsets) > 0 {
					publisher.PublishWatermark(processorWM, offsets[len(offsets)-1], int32(index))
					activeWatermarkBuffers[toVertexName][index] = true
					// reset because the toBuffer partition is no longer idling
					isdf.idleManager.MarkActive(isdf.fromBufferPartition.GetPartitionIdx(), isdf.toBuffers[toVertexName][index].GetName())
				}
				// This (len(offsets) == 0) happens at conditional forwarding, there's no data written to the buffer
			}
		}
	}
	// - condition1 "len(dataMessages) > 0" :
	//   Meaning, we do have some data messages, but we may not have written to all out buffers or its partitions.
	//   It could be all data messages are dropped, or conditional forwarding to part of the out buffers.
	//   If we don't have this condition check, when dataMessages is zero but ctrlMessages > 0, we will
	//   wrongly publish an idle watermark without the ctrl message and the ctrl message tracking map.
	// - condition 2 "len(activeWatermarkBuffers) < len(isdf.wmPublishers)" :
	//   send idle watermark only if we have idle out buffers
	// Note: When the len(dataMessages) is 0, meaning all the readMessages are control messages, we choose not to do extra steps
	// This is because, if the idle continues, we will eventually handle the idle watermark when we read the next batch where the len(readMessages) will be zero
	if len(dataMessages) > 0 {
		for bufferName := range isdf.wmPublishers {
			for index, activePartition := range activeWatermarkBuffers[bufferName] {
				if !activePartition {
					// use the watermark of the current read batch for the idle watermark
					// same as read len==0 because there's no event published to the buffer
					if p, ok := isdf.wmPublishers[bufferName]; ok {
						idlehandler.PublishIdleWatermark(ctx, isdf.fromBufferPartition.GetPartitionIdx(), isdf.toBuffers[bufferName][index], p, isdf.idleManager, isdf.opts.logger, isdf.vertexName, isdf.pipelineName, dfv1.VertexTypeMapUDF, isdf.vertexReplica, processorWM)
					}
				}
			}
		}
	}

	// when we apply udf, we don't handle partial errors (it's either non or all, non will return early),
	// so we should be able to ack all the readOffsets including data messages and control messages
	ackStart := time.Now()
	err = isdf.ackFromBuffer(ctx, readOffsets)
	// implicit return for posterity :-)
	if err != nil {
		isdf.opts.logger.Errorw("Failed to ack from buffer", zap.Error(err))
		metrics.AckMessageError.With(metricLabelsWithPartition).Add(float64(len(readOffsets)))
		return err
	}
	metrics.AckProcessingTime.With(metricLabelsWithPartition).Observe(float64(time.Since(ackStart).Microseconds()))
	metrics.AckMessagesCount.With(metricLabelsWithPartition).Add(float64(len(readOffsets)))

	if isdf.opts.cbPublisher != nil {
		// Publish the callback for the vertex
		if err = isdf.opts.cbPublisher.NonSinkVertexCallback(ctx, udfResults); err != nil {
			isdf.opts.logger.Errorw("Failed to publish callback", zap.Error(err))
		}
	}
	// ProcessingTimes of the entire forwardAChunk
	metrics.ForwardAChunkProcessingTime.With(metricLabels).Observe(float64(time.Since(start).Microseconds()))
	return nil
}

// streamMessage streams the data messages to the next step.
func (isdf *InterStepDataForward) streamMessage(ctx context.Context, dataMessages []*isb.ReadMessage) (map[string][][]isb.Offset, error) {
	// Initialize maps for messages and offsets
	messageToStep := make(map[string][][]isb.Message)
	writeOffsets := make(map[string][][]isb.Offset)

	metricLabels := map[string]string{
		metrics.LabelVertex:             isdf.vertexName,
		metrics.LabelPipeline:           isdf.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeMapUDF),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)),
	}
	metricLabelsWithPartition := map[string]string{
		metrics.LabelVertex:             isdf.vertexName,
		metrics.LabelPipeline:           isdf.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeMapUDF),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)),
		metrics.LabelPartitionName:      isdf.fromBufferPartition.GetName(),
	}

	for toVertex := range isdf.toBuffers {
		messageToStep[toVertex] = make([][]isb.Message, len(isdf.toBuffers[toVertex]))
		writeOffsets[toVertex] = make([][]isb.Offset, len(isdf.toBuffers[toVertex]))
	}

	// Ensure dataMessages length is 1 for streaming
	if len(dataMessages) != 1 {
		errMsg := "data message size is not 1 with map UDF streaming"
		isdf.opts.logger.Errorw(errMsg, zap.Int("dataMessagesSize", len(dataMessages)))
		return nil, errors.New(errMsg)
	}

	// Process the single data message
	start := time.Now()
	metrics.UDFReadMessagesCount.With(metricLabelsWithPartition).Inc()

	writeMessageCh := make(chan isb.WriteMessage)
	errs, ctx := errgroup.WithContext(ctx)
	errs.Go(func() error {
		return isdf.opts.streamMapUdfApplier.ApplyMapStream(ctx, dataMessages[0], writeMessageCh)
	})

	// Stream the message to the next vertex
	for writeMessage := range writeMessageCh {
		writeMessage.Headers = dataMessages[0].Headers
		metrics.UDFWriteMessagesCount.With(metricLabelsWithPartition).Add(1)

		// Determine where to step and write to buffers
		if err := isdf.whereToStep(&writeMessage, messageToStep, dataMessages[0]); err != nil {
			return nil, fmt.Errorf("failed at whereToStep, error: %w", err)
		}

		curWriteOffsets, err := isdf.writeToBuffers(ctx, messageToStep)
		if err != nil {
			return nil, fmt.Errorf("failed to write to toBuffers, error: %w", err)
		}

		// Merge current write offsets into the main writeOffsets map
		for vertexName, toVertexBufferOffsets := range curWriteOffsets {
			for index, offsets := range toVertexBufferOffsets {
				writeOffsets[vertexName][index] = append(writeOffsets[vertexName][index], offsets...)
			}
		}

		// Clear messageToStep, as we have written the messages to the buffers
		for toVertex := range isdf.toBuffers {
			messageToStep[toVertex] = make([][]isb.Message, len(isdf.toBuffers[toVertex]))
		}
	}

	// Handle errors in UDF processing
	if err := errs.Wait(); err != nil {
		metrics.UDFError.With(metricLabels).Inc()
		if ok, _ := isdf.IsShuttingDown(); ok {
			isdf.opts.logger.Errorw("mapUDF.Apply, Stop called while stuck on an internal error", zap.Error(err))
			metrics.PlatformError.With(metricLabels).Inc()
		}
		return nil, fmt.Errorf("failed to applyUDF, error: %w", err)
	}

	metrics.UDFProcessingTime.With(metricLabels).Observe(float64(time.Since(start).Microseconds()))

	return writeOffsets, nil
}

// ackFromBuffer acknowledges an array of offsets back to fromBufferPartition and is a blocking call or until shutdown has been initiated.
func (isdf *InterStepDataForward) ackFromBuffer(ctx context.Context, offsets []isb.Offset) error {
	var ackRetryBackOff = wait.Backoff{
		Factor:   1,
		Jitter:   0.1,
		Steps:    math.MaxInt,
		Duration: time.Millisecond * 10,
	}
	var ackOffsets = offsets
	attempt := 0

	ctxClosedErr := wait.ExponentialBackoff(ackRetryBackOff, func() (done bool, err error) {
		errs := isdf.fromBufferPartition.Ack(ctx, ackOffsets)
		attempt += 1
		summarizedErr := errorArrayToMap(errs)
		var failedOffsets []isb.Offset
		if len(summarizedErr) > 0 {
			isdf.opts.logger.Errorw("Failed to ack from buffer, retrying", zap.Any("errors", summarizedErr), zap.Int("attempt", attempt))
			// no point retrying if ctx.Done has been invoked
			select {
			case <-ctx.Done():
				// no point in retrying after we have been asked to stop.
				return false, ctx.Err()
			default:
				// retry only the failed offsets
				for i, offset := range ackOffsets {
					if errs[i] != nil {
						failedOffsets = append(failedOffsets, offset)
					}
				}
				ackOffsets = failedOffsets
				if ok, _ := isdf.IsShuttingDown(); ok {
					ackErr := fmt.Errorf("AckFromBuffer, Stop called while stuck on an internal error, %v", summarizedErr)
					return false, ackErr
				}
				return false, nil
			}
		} else {
			return true, nil
		}
	})

	if ctxClosedErr != nil {
		isdf.opts.logger.Errorw("Context closed while waiting to ack messages inside forward", zap.Error(ctxClosedErr))
	}

	return ctxClosedErr
}

// writeToBuffers is a blocking call until all the messages have be forwarded to all the toBuffers, or a shutdown
// has been initiated while we are stuck looping on an InternalError.
func (isdf *InterStepDataForward) writeToBuffers(
	ctx context.Context, messageToStep map[string][][]isb.Message,
) (writeOffsets map[string][][]isb.Offset, err error) {
	// messageToStep contains all the to buffers, so the messages could be empty (conditional forwarding).
	// So writeOffsets also contains all the to buffers, but the returned offsets might be empty.
	writeOffsets = make(map[string][][]isb.Offset)
	for toVertexName, toVertexMessages := range messageToStep {
		writeOffsets[toVertexName] = make([][]isb.Offset, len(toVertexMessages))
	}
	for toVertexName, toVertexBuffer := range isdf.toBuffers {
		for index, partition := range toVertexBuffer {
			writeOffsets[toVertexName][index], err = isdf.writeToBuffer(ctx, partition, messageToStep[toVertexName][index])
			if err != nil {
				return nil, err
			}
		}
	}
	return writeOffsets, nil
}

// writeToBuffer forwards an array of messages to a single buffer and is a blocking call or until shutdown has been initiated.
func (isdf *InterStepDataForward) writeToBuffer(ctx context.Context, toBufferPartition isb.BufferWriter, messages []isb.Message) (writeOffsets []isb.Offset, err error) {
	var (
		totalCount int
		writeCount int
		writeBytes float64
	)
	// initialize metric labels
	metricLabels := map[string]string{
		metrics.LabelVertex:             isdf.vertexName,
		metrics.LabelPipeline:           isdf.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeMapUDF),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)),
	}
	metricLabelsWithPartition := map[string]string{
		metrics.LabelVertex:             isdf.vertexName,
		metrics.LabelPipeline:           isdf.pipelineName,
		metrics.LabelVertexType:         string(dfv1.VertexTypeMapUDF),
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)),
		metrics.LabelPartitionName:      toBufferPartition.GetName(),
	}
	totalCount = len(messages)
	writeOffsets = make([]isb.Offset, 0, totalCount)
	writeStart := time.Now()

	for {
		_writeOffsets, errs := toBufferPartition.Write(ctx, messages)
		// Note: this is an unwanted memory allocation during a happy path. We want only minimal allocation since using failedMessages is an unlikely path.
		var failedMessages []isb.Message
		needRetry := false
		for idx, msg := range messages {
			if err = errs[idx]; err != nil {
				// ATM there are no user-defined errors during write, all are InternalErrors.
				// Non retryable error, drop the message. Non retryable errors are only returned
				// when the buffer is full and the user has set the buffer full strategy to
				// DiscardLatest or when the message is duplicate.
				if errors.As(err, &isb.NonRetryableBufferWriteErr{}) {
					metricLabelWithReason := map[string]string{
						metrics.LabelVertex:             isdf.vertexName,
						metrics.LabelPipeline:           isdf.pipelineName,
						metrics.LabelVertexType:         string(dfv1.VertexTypeSink),
						metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica)),
						metrics.LabelPartitionName:      toBufferPartition.GetName(),
						metrics.LabelReason:             err.Error(),
					}
					metrics.DropMessagesCount.With(metricLabelWithReason).Inc()
					metrics.DropBytesCount.With(metricLabelWithReason).Add(float64(len(msg.Payload)))
					isdf.opts.logger.Infow("Dropped message", zap.String("reason", err.Error()), zap.String("partition", toBufferPartition.GetName()), zap.String("vertex", isdf.vertexName), zap.String("pipeline", isdf.pipelineName), zap.String("msg_id", msg.ID.String()))
				} else {
					needRetry = true
					// we retry only failed messages
					failedMessages = append(failedMessages, msg)
					metrics.WriteMessagesError.With(metricLabelsWithPartition).Inc()
					// a shutdown can break the blocking loop caused due to InternalErr
					if ok, _ := isdf.IsShuttingDown(); ok {
						metrics.PlatformError.With(metricLabels).Inc()
						return writeOffsets, fmt.Errorf("writeToBuffer failed, Stop called while stuck on an internal error with failed messages:%d, %v", len(failedMessages), errs)
					}
				}
			} else {
				writeCount++
				writeBytes += float64(len(msg.Payload))
				// we support write offsets only for jetstream
				if _writeOffsets != nil {
					writeOffsets = append(writeOffsets, _writeOffsets[idx])
				}
			}
		}

		if needRetry {
			isdf.opts.logger.Errorw("Retrying failed messages",
				zap.Any("errors", errorArrayToMap(errs)),
				zap.String(metrics.LabelPipeline, isdf.pipelineName),
				zap.String(metrics.LabelVertex, isdf.vertexName),
				zap.String(metrics.LabelPartitionName, toBufferPartition.GetName()),
			)
			// set messages to failed for the retry
			messages = failedMessages
			// TODO: implement retry with backoff etc.
			time.Sleep(isdf.opts.retryInterval)
		} else {
			break
		}
	}

	metrics.WriteProcessingTime.With(metricLabelsWithPartition).Observe(float64(time.Since(writeStart).Microseconds()))
	metrics.WriteMessagesCount.With(metricLabelsWithPartition).Add(float64(writeCount))
	metrics.WriteBytesCount.With(metricLabelsWithPartition).Add(writeBytes)
	return writeOffsets, nil
}

// applyUDF applies the map UDF and will block if there is any InternalErr. On the other hand, if this is a UserError
// the skip flag is set. ShutDown flag will only if there is an InternalErr and ForceStop has been invoked.
// The UserError retry will be done on the ApplyUDF.
func (isdf *InterStepDataForward) applyUDF(ctx context.Context, readMessages []*isb.ReadMessage) ([]isb.ReadWriteMessagePair, error) {
	writeMessages, err := isdf.opts.unaryMapUdfApplier.ApplyMap(ctx, readMessages)
	if err != nil {
		isdf.opts.logger.Errorw("mapUDF.Apply error", zap.Error(err))
		return nil, err
	}
	return writeMessages, nil
}

// whereToStep executes the WhereTo interfaces and then updates the to step's writeToBuffers buffer.
func (isdf *InterStepDataForward) whereToStep(writeMessage *isb.WriteMessage, messageToStep map[string][][]isb.Message, readMessage *isb.ReadMessage) error {
	// call WhereTo and drop it on errors
	to, err := isdf.fsd.WhereTo(writeMessage.Keys, writeMessage.Tags, writeMessage.ID.String())
	if err != nil {
		isdf.opts.logger.Errorw("failed in whereToStep", zap.Error(isb.MessageWriteErr{Name: isdf.fromBufferPartition.GetName(), Header: readMessage.Header, Body: readMessage.Body, Message: fmt.Sprintf("WhereTo failed, %s", err)}))
		// a shutdown can break the blocking loop caused due to InternalErr
		if ok, _ := isdf.IsShuttingDown(); ok {
			err := fmt.Errorf("whereToStep, Stop called while stuck on an internal error, %v", err)
			metrics.PlatformError.With(map[string]string{metrics.LabelVertex: isdf.vertexName, metrics.LabelPipeline: isdf.pipelineName, metrics.LabelVertexType: string(dfv1.VertexTypeMapUDF), metrics.LabelVertexReplicaIndex: strconv.Itoa(int(isdf.vertexReplica))}).Inc()
			return err
		}
		return err
	}

	for _, t := range to {
		if _, ok := messageToStep[t.ToVertexName]; !ok {
			isdf.opts.logger.Errorw("failed in whereToStep", zap.Error(isb.MessageWriteErr{Name: isdf.fromBufferPartition.GetName(), Header: readMessage.Header, Body: readMessage.Body, Message: fmt.Sprintf("no such destination (%s)", t.ToVertexName)}))
		}
		messageToStep[t.ToVertexName][t.ToVertexPartitionIdx] = append(messageToStep[t.ToVertexName][t.ToVertexPartitionIdx], writeMessage.Message)
	}
	return nil
}

// errorArrayToMap summarizes an error array to map
func errorArrayToMap(errs []error) map[string]int64 {
	result := make(map[string]int64)
	for _, err := range errs {
		if err != nil {
			result[err.Error()]++
		}
	}
	return result
}
