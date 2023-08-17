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

package udsource

import (
	"context"

	"github.com/numaproj/numaflow/pkg/isb"
)

// TODO - udsource - use gRPC client to connect to the user defined source and implement the interfaces below
type userDefinedSource struct {
}

func New() (*userDefinedSource, error) {
	return &userDefinedSource{}, nil
}

func (u *userDefinedSource) GetName() string {
	return ""
}

// GetPartitionIdx returns the partition number for the user-defined source.
// Source is like a buffer with only one partition. So, we always return 0
func (u *userDefinedSource) GetPartitionIdx() int32 {
	return 0
}

func (u *userDefinedSource) Read(_ context.Context, count int64) ([]*isb.ReadMessage, error) {
	return nil, nil
}

func (u *userDefinedSource) Ack(_ context.Context, offsets []isb.Offset) []error {
	return make([]error, len(offsets))
}

func (u *userDefinedSource) NoAck(_ context.Context, _ []isb.Offset) {
	// User defined source does not support NoAck
}

func (u *userDefinedSource) Close() error {
	return nil
}

func (u *userDefinedSource) Stop() {
}

func (u *userDefinedSource) ForceStop() {
}

func (u *userDefinedSource) Start() <-chan struct{} {
	return nil
}
