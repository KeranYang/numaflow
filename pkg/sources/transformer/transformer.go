package transformer

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/forward/applier"
	"github.com/numaproj/numaflow/pkg/isb"
)

type Impl struct {
	transformer applier.MapApplier
	logger      *zap.SugaredLogger
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
	for _, m := range messages {
		tMessages := h.applyTransformer(ctx, m)
		for _, tm := range tMessages {
			rms = append(rms, tm.ToReadMessage(m.ReadOffset, m.Watermark))
		}
	}
	return rms
}

func (h *Impl) applyTransformer(ctx context.Context, readMessage *isb.ReadMessage) []*isb.Message {
	for {
		writeMessages, err := h.transformer.ApplyMap(ctx, readMessage)
		if err != nil {
			// keep retrying, I cannot think of a use case where a user could say, errors are fine :-)
			// as a platform we should not lose or corrupt data.
			h.logger.Errorw("Transformer.Apply error", zap.Error(err))
			// TODO: implement retry with backoff etc.
			time.Sleep(time.Minute)
			continue
		} else {
			// if we do not get a time from transformer, we set it to the time from input readMessage
			for _, m := range writeMessages {
				if m.EventTime.IsZero() {
					m.EventTime = readMessage.EventTime
				}
			}
			return writeMessages
		}
	}
}
