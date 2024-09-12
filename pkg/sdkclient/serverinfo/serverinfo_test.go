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

package serverinfo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var testMinimumSupportedSDKVersions = sdkConstraints{
	Go:     "0.6.0-0",
	Python: "0.6.0a",
	Java:   "0.6.0-0",
}

func TestCheckNumaflowCompatibility(t *testing.T) {
	tests := []struct {
		name               string
		numaflowVersion    string
		minNumaflowVersion string
		shouldErr          bool
		errMessage         string
	}{
		{
			name:               "Test with incompatible numaflow version",
			numaflowVersion:    "v1.1.6",
			minNumaflowVersion: "1.1.7",
			shouldErr:          true,
			errMessage:         "numaflow version 1.1.6 must be upgraded to at least 1.1.7, in order to work with current SDK version",
		},
		{
			name:               "Test with empty MinimumNumaflowVersion field",
			numaflowVersion:    "1.1.7",
			minNumaflowVersion: "",
			shouldErr:          true,
			errMessage:         "server info does not contain minimum numaflow version. Upgrade to newer SDK version",
		},
		{
			name:               "Test with compatible numaflow version",
			numaflowVersion:    "1.1.7",
			minNumaflowVersion: "1.1.6",
			shouldErr:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkNumaflowCompatibility(tt.numaflowVersion, tt.minNumaflowVersion)
			if tt.shouldErr {
				assert.Error(t, err, "Expected error")
				assert.Contains(t, err.Error(), tt.errMessage)
			} else {
				assert.NoError(t, err, "Expected no error")
			}
		})
	}
}

func TestCheckSDKCompatibility(t *testing.T) {
	tests := []struct {
		name                        string
		sdkVersion                  string
		sdkLanguage                 Language
		minimumSupportedSDKVersions sdkConstraints
		shouldErr                   bool
		errMessage                  string
	}{
		{
			name:                        "Test with incompatible Python version",
			sdkVersion:                  "v0.5.3a1",
			sdkLanguage:                 Python,
			minimumSupportedSDKVersions: testMinimumSupportedSDKVersions,
			shouldErr:                   true,
			errMessage:                  "SDK version 0.5.3a1 must be upgraded to at least 0.6.0a, in order to work with current numaflow version",
		},
		{
			name:                        "Test with compatible Python version",
			sdkVersion:                  "v0.6.0a2",
			sdkLanguage:                 Python,
			minimumSupportedSDKVersions: testMinimumSupportedSDKVersions,
			shouldErr:                   false,
		},
		{
			name:                        "Test with incompatible Java version",
			sdkVersion:                  "v0.4.3",
			sdkLanguage:                 Java,
			minimumSupportedSDKVersions: testMinimumSupportedSDKVersions,
			shouldErr:                   true,
			errMessage:                  "SDK version 0.4.3 must be upgraded to at least 0.6.0-0, in order to work with current numaflow version",
		},
		{
			name:                        "Test with compatible Go version",
			sdkVersion:                  "v0.6.0-rc2",
			sdkLanguage:                 Go,
			minimumSupportedSDKVersions: testMinimumSupportedSDKVersions,
			shouldErr:                   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkSDKCompatibility(tt.sdkVersion, tt.sdkLanguage, tt.minimumSupportedSDKVersions)
			if tt.shouldErr {
				assert.Error(t, err, "Expected error")
				assert.Contains(t, err.Error(), tt.errMessage)
			} else {
				assert.NoError(t, err, "Expected no error")
			}
		})
	}
}
