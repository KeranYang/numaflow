package transformer

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/forward/applier"
	"github.com/numaproj/numaflow/pkg/isb"
)

type Impl struct {
	transformer applier.MapApplier
	logger      *zap.SugaredLogger
}

// tracker tracks an execution of a data transformation.
type tracker struct {
	// readMessage is the message passed to the transformer.
	readMessage *isb.ReadMessage
	// transformedMessages are list of messages returned by transformer.
	transformedMessages []*isb.ReadMessage
	// transformError is the error thrown by transformer.
	transformError error
}

func New(
	transformer applier.MapApplier,
	logger *zap.SugaredLogger,
) *Impl {
	i := &Impl{
		transformer: transformer,
		logger:      logger,
	}
	return i
}

func (h *Impl) Transform(ctx context.Context, messages []*isb.ReadMessage) []*isb.ReadMessage {
	var rms []*isb.ReadMessage
	// Transformer concurrent processing request channel
	transformCh := make(chan *tracker)
	// transformTrackers stores the results after transformer processing for all read messages.
	transformTrackers := make([]tracker, len(messages))

	var wg sync.WaitGroup
	// TODO - configurable concurrency number instead of 100
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.concurrentApplyTransformer(ctx, transformCh)
		}()
	}

	concurrentProcessingStart := time.Now()
	// send transformer processing work to the channel
	for idx, readMessage := range messages {
		transformTrackers[idx].readMessage = readMessage
		transformCh <- &transformTrackers[idx]
	}
	// let the go routines know that there is no more work
	close(transformCh)
	// wait till the processing is done. this will not be an infinite wait because the transformer processing will exit if
	// context.Done() is closed.
	wg.Wait()
	h.logger.Debugw("concurrent applyTransformer completed", zap.Int("concurrency", 100), zap.Duration("took", time.Since(concurrentProcessingStart)))
	// TODO - emit metrics on concurrent processing time

	// Transformer processing is done, construct return list.
	for _, m := range transformTrackers {
		// look for errors, if we see even 1 error let's return. Handling partial retrying is not worth ATM.
		if m.transformError != nil {
			// TODO - emit transformer error metrics.
			h.logger.Errorw("failed to applyTransformer", zap.Error(m.transformError))
			// If error skip processing this particular message?
			continue
		}
		rms = append(rms, m.transformedMessages...)
	}
	return rms
}

// concurrentApplyTransformer applies the transformer based on the request from the channel
func (h *Impl) concurrentApplyTransformer(ctx context.Context, tracker <-chan *tracker) {
	for t := range tracker {
		// TODO - emit metrics on transformer.
		transformedMessages := h.applyTransformer(ctx, t.readMessage)
		t.transformedMessages = append(t.transformedMessages, transformedMessages...)
		t.transformError = nil
	}
}

func (h *Impl) applyTransformer(ctx context.Context, readMessage *isb.ReadMessage) []*isb.ReadMessage {
	var transformedMessages []*isb.ReadMessage
	for {
		msgs, err := h.transformer.ApplyMap(ctx, readMessage)
		if err != nil {
			// keep retrying, I cannot think of a use case where a user could say, errors are fine :-)
			// as a platform we should not lose or corrupt data.
			h.logger.Errorw("Transformer.Apply error", zap.Error(err))
			// TODO: implement retry with backoff etc.
			time.Sleep(time.Minute)
			continue
		} else {
			// if we do not get a time from transformer, we set it to the time from input readMessage
			for _, m := range msgs {
				if m.EventTime.IsZero() {
					m.EventTime = readMessage.EventTime
				}

				// Construct isb.ReadMessage from isb.Message by providing ReadOffset and Watermark.
				// For ReadOffset, we inherit from parent ReadMessage for now. This will cause multiple ReadMessages sharing exact same ReadOffset.
				// Should we append index to offset to make it unique?

				// For Watermark, we also inherit from parent, which is ok because
				// after transformer assigns new event time to messages, sourcer will accordingly publish new watermarks to fromBuffer and
				// data forwarder will use source watermark fetcher to fetch watermark and reassign it to each of the ReadMessages.
				transformedMessages = append(transformedMessages, m.ToReadMessage(readMessage.ReadOffset, readMessage.Watermark))
			}
			return transformedMessages
		}
	}
}
