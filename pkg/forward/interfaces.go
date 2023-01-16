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

package forward

import (
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

var (
	All  = GoWhere(func(string) ([]string, error) { return []string{dfv1.MessageKeyAll}, nil })
	Drop = GoWhere(func(string) ([]string, error) { return []string{dfv1.MessageKeyDrop}, nil })
)

// ToWhichStepDecider decides which step to forward after applying the WhereTo function.
type ToWhichStepDecider interface {
	// WhereTo decides where to forward the result to based on the name of the step it returns.
	// It supports 2 addition keywords which need not be a step name. They are "ALL" and "DROP"
	// where former means, forward to all the neighbouring steps and latter means do not forward anywhere.
	WhereTo(string) ([]string, error)
}

// GoWhere is the step decider on where it needs to go
type GoWhere func(string) ([]string, error)

// WhereTo decides where the data goes to.
func (gw GoWhere) WhereTo(b string) ([]string, error) {
	return gw(b)
}

// StarterStopper starts/stops the forwarding.
type StarterStopper interface {
	// TODO - do we need context here?
	Start() <-chan struct{}
	// TODO - do we need context here?
	Stop()
	// TODO - do we need context here?
	ForceStop()
}
