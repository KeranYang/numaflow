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
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"net/http"
	"strings"
)

func init() {
	// TODO - can try using POST instead of get - see kafka example.
	http.HandleFunc("/http/send-message", func(w http.ResponseWriter, r *http.Request) {
		pName := r.URL.Query().Get("pName")
		vertexName := r.URL.Query().Get("vName")
		msg := r.URL.Query().Get("msg")

		restConfig, err := rest.InClusterConfig()
		if err != nil {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(err.Error()))
			// panic(err)
		}

		kubeClient, err := kubernetes.NewForConfig(restConfig)
		if err != nil {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(err.Error()))
			// panic(err)
		}

		labelSelector := fmt.Sprintf("%s=%s,%s=%s", dfv1.KeyPipelineName, pName, dfv1.KeyVertexName, vertexName)
		ctx := context.Background()
		podList, err := kubeClient.CoreV1().Pods("numaflow-system").List(ctx, metav1.ListOptions{LabelSelector: labelSelector, FieldSelector: "status.phase=Running"})
		if err != nil {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(err.Error()))
			// panic(err)
		}
		pod := podList.Items[0]
		podIp := pod.Status.PodIP
		// Send the msg to the input vertex.
		resp, err := http.Post("https://"+podIp+":8443/vertices/in", "application/json", strings.NewReader(msg))
		if err != nil {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(err.Error()))
			// panic(err)
		}

		defer resp.Body.Close()

		if resp.StatusCode >= 300 {
			w.WriteHeader(resp.StatusCode)
			return
		}
		w.WriteHeader(201)
	})
}
