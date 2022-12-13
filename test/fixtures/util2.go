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
)

func SinkOutputNotContains(ctx context.Context, sinkName string, regex string) bool {
	return !sinkOutputContains(ctx, sinkName, regex, 1)
}

func SinkOutputContains(ctx context.Context, sinkName string, targetRegex string, opts ...SinkCheckOption) bool {
	o := defaultSinkCheckOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(o)
		}
	}
	return sinkOutputContains(ctx, sinkName, targetRegex, o.count)

}

func sinkOutputContains(ctx context.Context, sinkName string, targetRegex string, expectedCount int) bool {
	return GetMsgCountContains(sinkName, targetRegex) >= expectedCount
}

type sinkCheckOptions struct {
	count int
}

func defaultSinkCheckOptions() *sinkCheckOptions {
	return &sinkCheckOptions{
		count: 1,
	}
}

type SinkCheckOption func(*sinkCheckOptions)

func SinkCheckOptionWithCount(c int) SinkCheckOption {
	return func(o *sinkCheckOptions) {
		o.count = c
	}
}
