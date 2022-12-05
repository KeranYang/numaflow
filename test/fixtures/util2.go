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

package fixtures

import (
	"context"
	"fmt"
	"time"
)

func SinkOutputNotContains(ctx context.Context, sinkName string, regex string, opts ...SinkCheckOption) bool {
	o := defaultSinkCheckOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(o)
		}
	}
	ctx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()

	if o.count <= 0 {
		o.count = 1
	}

	return !sinkOutputContains(ctx, sinkName, regex, o.count)
}

func SinkOutputContains(ctx context.Context, sinkName string, regex string, opts ...SinkCheckOption) bool {
	o := defaultSinkCheckOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(o)
		}
	}
	ctx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()

	return sinkOutputContains(ctx, sinkName, regex, o.count)

}

func sinkOutputContains(ctx context.Context, sinkName string, regex string, expectedCount int) bool {
	if expectedCount <= 0 {
		return true
	}

	for {
		select {
		case <-ctx.Done():
			// Timeout.
			fmt.Printf("KeranTest - timed out.")
			return false
		default:
			return GetMsgCountContains(sinkName, regex) >= expectedCount
		}
	}
}

type sinkCheckOptions struct {
	timeout time.Duration
	count   int
}

func defaultSinkCheckOptions() *sinkCheckOptions {
	return &sinkCheckOptions{
		timeout: defaultTimeout,
		count:   -1,
	}
}

type SinkCheckOption func(*sinkCheckOptions)

func SinkCheckOptionWithTimeout(t time.Duration) SinkCheckOption {
	return func(o *sinkCheckOptions) {
		o.timeout = t
	}
}

func SinkCheckOptionWithCount(c int) SinkCheckOption {
	return func(o *sinkCheckOptions) {
		o.count = c
	}
}
