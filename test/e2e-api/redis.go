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

package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"net/http"
)

func init() {
	http.HandleFunc("/redis/get-msg-count-contains", func(w http.ResponseWriter, r *http.Request) {
		sinkName := r.URL.Query().Get("sinkName")
		targetStr := r.URL.Query().Get("targetStr")

		// Currently E2E tests share the same redis instance, in the future we can consider passing in redis configurations
		// to enable REST backend sending requests to specified redis instance.
		client := redis.NewClient(&redis.Options{
			Addr:     "redis-cluster:6379",
			Password: "",
			DB:       0,
		})

		keyList, err := client.Keys(context.Background(), fmt.Sprintf("%s*%s*", sinkName, targetStr)).Result()
		if err != nil {
			fmt.Printf("KeranTest - panic err %v", err)
			panic(err)
		}

		fmt.Printf("KeranTest - got key list %v", keyList)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fmt.Sprint(len(keyList))))
	})
}
