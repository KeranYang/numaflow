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

package validator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidatePipelineCreate(t *testing.T) {
	pipeline := fakePipeline()
	fk := MockInterStepBufferServices{}
	v := NewPipelineValidator(&fk, nil, pipeline)
	r := v.ValidateCreate(contextWithLogger(t))
	assert.True(t, r.Allowed)
}

func TestValidatePipelineUpdate(t *testing.T) {
	pipeline := fakePipeline()
	fk := MockInterStepBufferServices{}
	t.Run("test invalid new pipeline spec", func(t *testing.T) {
		v := NewPipelineValidator(&fk, pipeline, nil)
		r := v.ValidateUpdate(contextWithLogger(t))
		assert.False(t, r.Allowed)
	})
	t.Run("test invalid old pipeline spec", func(t *testing.T) {
		v := NewPipelineValidator(&fk, nil, pipeline)
		r := v.ValidateUpdate(contextWithLogger(t))
		assert.False(t, r.Allowed)
	})
	t.Run("test Pipeline interStepBufferServiceName change", func(t *testing.T) {
		newPipeline := pipeline.DeepCopy()
		newPipeline.Spec.InterStepBufferServiceName = "change-name"
		v := NewPipelineValidator(&fk, pipeline, newPipeline)
		r := v.ValidateUpdate(contextWithLogger(t))
		assert.False(t, r.Allowed)
	})
}
