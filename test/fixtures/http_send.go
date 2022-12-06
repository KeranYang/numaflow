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
	"log"
)

func SendMessageToPod(pIp string, msg string) string {
	log.Printf("Sending msg %s to pod ip %s\n", msg, pIp)
	InvokeE2EAPI("/http/send-message?ip=%s&msg=%s", pIp, msg)
	return msg
}
