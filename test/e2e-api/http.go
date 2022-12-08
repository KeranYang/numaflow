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
	"log"
	"net/http"
	"time"
)

func init() {
	http.HandleFunc("/http/send-message", func(w http.ResponseWriter, r *http.Request) {
		pName := r.URL.Query().Get("pipeline")
		vertexName := r.URL.Query().Get("vertex")
		log.Printf("KeranTest - Received, pipelineName is %s, vertexName is %s.", pName, vertexName)
		buf, err := io.ReadAll(r.Body)
		log.Printf("KeranTest - Received byte array. %v", buf)
		log.Printf("KeranTest - Converting to string. %s", string(buf[:]))
		if err != nil {
			panic(err)
		}

		log.Printf("Sending request to %s", fmt.Sprintf("https://%s-%s.numaflow-system.svc.cluster.local:8443/vertices/%s", pName, vertexName, vertexName))
		// https://even-odd-in.numaflow-system.svc.cluster.local:8443/vertices/in

		httpClient := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: time.Second * 3,
		}

		response, err := httpClient.Post(fmt.Sprintf("https://%s-%s:8443/vertices/%s",
			pName, vertexName, vertexName), "application/json", bytes.NewBuffer(buf))

		/*
			response, err := http.Post(
				fmt.Sprintf("https://%s-%s:8443/vertices/%s",
					pName, vertexName, vertexName), "application/json", bytes.NewBuffer(buf))
		*/

		if err != nil {
			panic(err)
		}
		log.Printf("KeranTest - post response: %v", *response)

		/*
			restConfig, err := rest.InClusterConfig()
			if err != nil {
				panic(err)
			}

			kubeClient, err := kubernetes.NewForConfig(restConfig)
			if err != nil {
				panic(err)
			}

			labelSelector := fmt.Sprintf("%s=%s,%s=%s", dfv1.KeyPipelineName, pName, dfv1.KeyVertexName, vertexName)

			log.Printf("labelSelector is %s", labelSelector)

			ctx := context.Background()

			podList, err := kubeClient.CoreV1().Pods("numaflow-system").List(ctx, metav1.ListOptions{LabelSelector: labelSelector, FieldSelector: "status.phase=Running"})
			if err != nil {
				// bingo - Solved by granting the sa permission to list pods.
				// pods is forbidden: User "system:serviceaccount:numaflow-system:default"
				// cannot list resource "pods" in API group "" in the namespace "numaflow-system"
				panic(err)
			}
			pod := podList.Items[0]
			podIp := pod.Status.PodIP
			log.Printf("podIP is %s", podIp)
			// Send the msg to the input vertex.
			// _, err = https.Post("http://"+podIp+":8443/vertices/in", "application/json", bytes.NewBuffer(buf))

		*/

	})
}
