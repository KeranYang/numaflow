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
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
)

func init() {
	http.HandleFunc("/http/send-message", func(w http.ResponseWriter, r *http.Request) {
		pName := r.URL.Query().Get("pipeline")
		vName := r.URL.Query().Get("vertex")

		buf, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(err.Error()))
			panic(err)
		}

		httpClient := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		}

		// TODO - It's observed that post doesn't always succeed (sometimes connection refused error), surround it by retry.
		_, err = httpClient.Post(fmt.Sprintf("https://%s-%s:8443/vertices/%s", pName, vName, vName), "application/json", bytes.NewBuffer(buf))
		if err != nil {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(err.Error()))
			panic(err)
		}
	})
}
