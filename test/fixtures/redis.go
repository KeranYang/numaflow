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
	"log"
	"time"
)

func GetRedisString(key string) string {
	str := InvokeE2EAPI("/redis/get-string?key=%s", key)
	return str
}

func ExpectRedisKeyValue(key string, expectedValue string, timeout time.Duration) {
	log.Printf("expecting key value pair in redis: key %s, value %s within %v\n", key, expectedValue, timeout)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			panic(fmt.Errorf("timeout waiting"))
		default:
			valueInRedis := GetRedisString(key)
			if valueInRedis == expectedValue {
				log.Printf("Received value %s, succeeding the test.", valueInRedis)
				return
			}
			time.Sleep(time.Second)
		}
	}
}
