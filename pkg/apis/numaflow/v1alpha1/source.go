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

package v1alpha1

import (
	"encoding/base64"
	"fmt"
	"sort"
	"strings"

	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

type Source struct {
	// +optional
	Generator *GeneratorSource `json:"generator,omitempty" protobuf:"bytes,1,opt,name=generator"`
	// +optional
	Kafka *KafkaSource `json:"kafka,omitempty" protobuf:"bytes,2,opt,name=kafka"`
	// +optional
	HTTP *HTTPSource `json:"http,omitempty" protobuf:"bytes,3,opt,name=http"`
	// +optional
	Nats *NatsConfig `json:"nats,omitempty" protobuf:"bytes,4,opt,name=nats"`
	// +optional
	RedisStreams *RedisStreamsSource `json:"redisStreams,omitempty" protobuf:"bytes,5,opt,name=redisStreams"`
	// +optional
	UDTransformer *UDTransformer `json:"transformer,omitempty" protobuf:"bytes,6,opt,name=transformer"`
	// +optional
	UDSource *UDSource `json:"udsource,omitempty" protobuf:"bytes,7,opt,name=udSource"`
}

func (s Source) getContainers(req getContainerReq) ([]corev1.Container, error) {
	containers := []corev1.Container{
		s.getMainContainer(req),
	}
	if s.UDTransformer != nil {
		containers = append(containers, s.getUDTransformerContainer(req))
	}
	if s.UDSource != nil {
		containers = append(containers, s.getUDSourceContainer(req))
	}
	// NATS source is a special case, it uses the user-defined nats image, but it's not specified as UDSource.
	if s.Nats != nil {
		containers = append(containers, s.getNatsContainer(req))
	}
	return containers, nil
}

func (s Source) getMainContainer(req getContainerReq) corev1.Container {
	return containerBuilder{}.init(req).args("processor", "--type="+string(VertexTypeSource), "--isbsvc-type="+string(req.isbSvcType)).build()
}

func (s Source) getUDTransformerContainer(mainContainerReq getContainerReq) corev1.Container {
	c := containerBuilder{}.
		name(CtrUdtransformer).
		imagePullPolicy(mainContainerReq.imagePullPolicy). // Use the same image pull policy as the main container
		appendVolumeMounts(mainContainerReq.volumeMounts...)
	if x := s.UDTransformer.Container; x != nil && x.Image != "" { // customized image
		c = c.image(x.Image)
		if len(x.Command) > 0 {
			c = c.command(x.Command...)
		}
		if len(x.Args) > 0 {
			c = c.args(x.Args...)
		}
	} else { // built-in
		args := []string{"builtin-transformer", "--name=" + s.UDTransformer.Builtin.Name}
		for _, a := range s.UDTransformer.Builtin.Args {
			args = append(args, "--args="+base64.StdEncoding.EncodeToString([]byte(a)))
		}
		var kwargs []string
		for k, v := range s.UDTransformer.Builtin.KWArgs {
			kwargs = append(kwargs, fmt.Sprintf("%s=%s", k, base64.StdEncoding.EncodeToString([]byte(v))))
		}
		if len(kwargs) > 0 {
			// The order of the kwargs items is random because we construct it from an unordered map Builtin.KWArgs.
			// We sort the kwargs first before converting it to a string argument to ensure consistency.
			// This is important because in vertex controller we use hash on PodSpec to determine if a pod already exists, which requires the kwargs being consistent.
			sort.Strings(kwargs)
			args = append(args, "--kwargs="+strings.Join(kwargs, ","))
		}

		c = c.image(mainContainerReq.image).args(args...) // Use the same image as the main container
	}
	if x := s.UDTransformer.Container; x != nil {
		c = c.appendEnv(x.Env...).appendVolumeMounts(x.VolumeMounts...).resources(x.Resources).securityContext(x.SecurityContext).appendEnvFrom(x.EnvFrom...)
		if x.ImagePullPolicy != nil {
			c = c.imagePullPolicy(*x.ImagePullPolicy)
		}
	}
	container := c.build()
	container.LivenessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   "/sidecar-livez",
				Port:   intstr.FromInt(VertexMetricsPort),
				Scheme: corev1.URISchemeHTTPS,
			},
		},
		InitialDelaySeconds: 30,
		PeriodSeconds:       60,
		TimeoutSeconds:      30,
	}
	return container
}

func (s Source) getUDSourceContainer(mainContainerReq getContainerReq) corev1.Container {
	c := containerBuilder{}.
		name(CtrUdsource).
		imagePullPolicy(mainContainerReq.imagePullPolicy). // Use the same image pull policy as the main container
		appendVolumeMounts(mainContainerReq.volumeMounts...)
	if x := s.UDSource.Container; x != nil && x.Image != "" { // customized image
		c = c.image(x.Image)
		if len(x.Command) > 0 {
			c = c.command(x.Command...)
		}
		if len(x.Args) > 0 {
			c = c.args(x.Args...)
		}
	}
	if x := s.UDSource.Container; x != nil {
		c = c.appendEnv(x.Env...).appendVolumeMounts(x.VolumeMounts...).resources(x.Resources).securityContext(x.SecurityContext).appendEnvFrom(x.EnvFrom...)
		if x.ImagePullPolicy != nil {
			c = c.imagePullPolicy(*x.ImagePullPolicy)
		}
	}
	container := c.build()
	container.LivenessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   "/sidecar-livez",
				Port:   intstr.FromInt(VertexMetricsPort),
				Scheme: corev1.URISchemeHTTPS,
			},
		},
		InitialDelaySeconds: 30,
		PeriodSeconds:       60,
		TimeoutSeconds:      30,
	}
	return container
}

func (s Source) getNatsContainer(mainContainerReq getContainerReq) corev1.Container {
	c := containerBuilder{}.
		name(CtrNatsSource).
		imagePullPolicy(mainContainerReq.imagePullPolicy). // Use the same image pull policy as the main container
		appendVolumeMounts(mainContainerReq.volumeMounts...)
	// TODO - ALL knowledge from user-defined nats package should be imported instead of hard-coded here
	c.Image = "quay.io/numaio/numaflow-source/nats-source:v0.5.4"
	nats := s.Nats
	if nats.Auth != nil && nats.Auth.Token != nil {
		// TODO - add complete volume list for nats authentications and TLS
		c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{
			// TODO - "my-secret-mount" and "/etc/secrets" should be const from the user-defined NATS package
			Name:      "my-secret-mount",
			MountPath: "/etc/secrets/" + nats.Auth.Token.Name,
		})
	}
	c.Env = append(c.Env, s.natsUDSourceEnvs()...)
	container := c.build()
	container.LivenessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   "/sidecar-livez",
				Port:   intstr.FromInt(VertexMetricsPort),
				Scheme: corev1.URISchemeHTTPS,
			},
		},
		InitialDelaySeconds: 30,
		PeriodSeconds:       60,
		TimeoutSeconds:      30,
	}
	return container
}

// natsUDSourceEnvs returns envs for configuring a nats source vertex.
func (s Source) natsUDSourceEnvs() []corev1.EnvVar {
	if s.Nats == nil {
		return []corev1.EnvVar{}
	}
	// TODO - The names should be a const from the user-defined NATS package
	return []corev1.EnvVar{
		{Name: "CONFIG_FORMAT", Value: "yaml"},
		{Name: "NATS_CONFIG", Value: stringify(s.Nats)},
	}
}

func stringify(x *NatsConfig) string {
	b, _ := yaml.Marshal(x)
	return string(b)
}
