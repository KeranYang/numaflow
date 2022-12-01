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
)

func init() {

	// const bootstrapServers = "kafka-broker:9092"
	// var brokers = []string{bootstrapServers}

	http.HandleFunc("/redis/get-string", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")

		client := redis.NewClient(&redis.Options{
			Addr:     "redis-cluster:6379",
			Password: "",
			DB:       0,
		})

		pong, err := client.Ping(context.Background()).Result()

		if err != nil {
			log.Fatalf("KeranTest - error %v", err)
		}

		value := key + "-" + pong
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fmt.Sprint(value)))
	})

	http.HandleFunc("/redis/get-total-key-count", func(w http.ResponseWriter, r *http.Request) {
		client := redis.NewClient(&redis.Options{
			Addr:     "redis-cluster:6379",
			Password: "",
			DB:       0,
		})

		keyList, err := client.Keys(context.Background(), "*").Result()

		log.Printf("KeranTest - error got key list, size %d", len(keyList))
		if err != nil {
			log.Fatalf("KeranTest - error %v", err)
		}

		count := len(keyList)
		log.Printf("KeranTest - got %d keys.", count)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fmt.Sprint(count)))
	})
}
