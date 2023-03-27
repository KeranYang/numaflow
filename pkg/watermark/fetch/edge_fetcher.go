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

package fetch

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

// edgeFetcher is a fetcher between two vertices.
type edgeFetcher struct {
	ctx              context.Context
	bufferName       string
	storeWatcher     store.WatermarkStoreWatcher
	processorManager *ProcessorManager
	log              *zap.SugaredLogger
}

// NewEdgeFetcher returns a new edge fetcher.
func NewEdgeFetcher(ctx context.Context, bufferName string, storeWatcher store.WatermarkStoreWatcher) Fetcher {
	log := logging.FromContext(ctx).With("bufferName", bufferName)
	log.Info("Creating a new edge watermark fetcher")
	return &edgeFetcher{
		ctx:              ctx,
		bufferName:       bufferName,
		storeWatcher:     storeWatcher,
		processorManager: NewProcessorManager(ctx, storeWatcher),
		log:              log,
	}
}

// GetWatermark gets the smallest timestamp for the given offset
func (e *edgeFetcher) GetWatermark(inputOffset isb.Offset) wmb.Watermark {
	var offset, err = inputOffset.Sequence()
	if err != nil {
		e.log.Errorw("Unable to get offset from isb.Offset.Sequence()", zap.Error(err))
		return wmb.Watermark(time.Unix(-1, 0))
	}
	var debugString strings.Builder
	var epoch int64 = math.MaxInt64
	var allProcessors = e.processorManager.GetAllProcessors()
	for _, p := range allProcessors {
		debugString.WriteString(fmt.Sprintf("[Processor: %v] \n", p))
		var t = p.offsetTimeline.GetEventTime(inputOffset)
		if t == -1 { // watermark cannot be computed, perhaps a new processing unit was added or offset fell off the timeline
			epoch = t
		} else if t < epoch {
			epoch = t
		}
		if p.IsDeleted() && (offset > p.offsetTimeline.GetHeadOffset()) {
			// if the pod is not active and the current offset is ahead of all offsets in Timeline
			e.processorManager.DeleteProcessor(p.entity.GetName())
		}
	}
	// if there are no processors
	if epoch == math.MaxInt64 {
		epoch = -1
	}
	e.log.Debugf("%s[%s] get watermark for offset %d: %+v", debugString.String(), e.bufferName, offset, epoch)

	return wmb.Watermark(time.UnixMilli(epoch))
}

// GetHeadWatermark returns the latest watermark among all processors. This can be used in showing the watermark
// progression for a vertex when not consuming the messages directly (eg. UX, tests)
// NOTE
//   - We don't use this function in the regular pods in the vertex.
//   - UX only uses GetHeadWatermark, so the `p.IsDeleted()` check in the GetWatermark never happens.
//     Meaning, in the UX (daemon service) we never delete any processor.
func (e *edgeFetcher) GetHeadWatermark() wmb.Watermark {
	var debugString strings.Builder
	var headOffset int64 = math.MinInt64
	var headWatermark int64 = math.MaxInt64
	var allProcessors = e.processorManager.GetAllProcessors()
	// get the head offset of each processor
	for _, p := range allProcessors {
		if !p.IsActive() {
			continue
		}
		var w = p.offsetTimeline.GetHeadWMB()
		e.log.Debugf("Processor: %v (headOffset:%d) (headWatermark:%d) (headIdle:%t)", p, w.Offset, w.Watermark, w.Idle)
		debugString.WriteString(fmt.Sprintf("[Processor:%v] (headOffset:%d) (headWatermark:%d) (headIdle:%t) \n", p, w.Offset, w.Watermark, w.Idle))
		if w.Offset != -1 {
			// find the smallest head offset of the smallest watermark
			if w.Watermark < headWatermark {
				headOffset = w.Offset
				headWatermark = w.Watermark
			} else if w.Watermark == headWatermark && w.Offset < headOffset {
				headOffset = w.Offset
			}
		}
	}
	e.log.Debugf("GetHeadWatermark: %s", debugString.String())
	if headWatermark == math.MaxInt64 {
		// Use -1 as default watermark value to indicate there is no valid watermark yet.
		return wmb.Watermark(time.UnixMilli(-1))
	}
	return wmb.Watermark(time.UnixMilli(headWatermark))
}

// GetHeadWMB returns the latest idle WMB among all processors
func (e *edgeFetcher) GetHeadWMB() wmb.WMB {
	var headWMB = wmb.WMB{
		// we find the head WMB only based on Offset
		Offset: math.MinInt64,
	}
	// if any head wmb from Vn-1 processors is not idle, we skip publishing
	var allProcessors = e.processorManager.GetAllProcessors()
	for _, p := range allProcessors {
		if !p.IsActive() {
			continue
		}
		var curHeadWMB = p.offsetTimeline.GetHeadWMB()
		if !curHeadWMB.Idle {
			return wmb.WMB{}
		}
		if curHeadWMB.Offset != -1 && curHeadWMB.Offset > headWMB.Offset {
			headWMB = curHeadWMB
		}
	}
	if headWMB.Offset == math.MinInt64 {
		// there is no valid watermark yet
		return wmb.WMB{}
	}
	return headWMB
}

// Close function closes the watchers.
func (e *edgeFetcher) Close() error {
	e.log.Infof("Closing edge watermark fetcher")
	if e.storeWatcher != nil {
		e.storeWatcher.HeartbeatWatcher().Close()
		e.storeWatcher.OffsetTimelineWatcher().Close()
	}
	return nil
}
