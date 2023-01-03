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
	"log"
	"net/http"
	"net/url"
)

var redisClient *redis.Client

func init() {

	// When we use this API to validate e2e test result, we always assume a redis UDSink is used
	// to persist data to a redis instance listening on port 6379.
	redisClient = redis.NewClient(&redis.Options{
		Addr: "redis:6379",
	})

	// get-msg-count-contains returns no. of occurrences of the targetStr in redis that are written by pipelineName and sinkName.
	http.HandleFunc("/redis/get-msg-count-contains", func(w http.ResponseWriter, r *http.Request) {
		pipelineName := r.URL.Query().Get("pipelineName")
		sinkName := r.URL.Query().Get("sinkName")
		targetStr, err := url.QueryUnescape(r.URL.Query().Get("targetStr"))
		if err != nil {
			log.Println(err)
			w.WriteHeader(400)
			_, _ = w.Write([]byte(err.Error()))
			return
		}

		count, err := redisClient.HGet(context.Background(), fmt.Sprintf("%s:%s", pipelineName, sinkName), targetStr).Result()

		if err != nil {
			log.Println(err)
			// If targetStr doesn't exist in the hash, HGet returns an error, meaning count is 0.
			w.WriteHeader(200)
			_, _ = w.Write([]byte("0"))
			return
		}

		w.WriteHeader(200)
		_, _ = w.Write([]byte(count))
	})
}
