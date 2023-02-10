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

package transformer

import (
	"context"
	"reflect"
	"testing"

	"github.com/numaproj/numaflow/pkg/forward/applier"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

func TestTransformer(t *testing.T) {
	tests := []struct {
		name         string
		ctx          context.Context
		mapper       applier.MapApplier
		input        []*isb.ReadMessage
		expectOutput []*isb.ReadMessage
	}{
		{
			name:         "",
			ctx:          context.Background(),
			mapper:       nil,
			input:        nil,
			expectOutput: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer := New(tt.mapper, logging.NewLogger())
			got := transformer.Transform(tt.ctx, tt.input)
			if !reflect.DeepEqual(got, tt.expectOutput) {
				t.Errorf("Transform() got = %v, want %v", got, tt.expectOutput)
			}
		})
	}
}
