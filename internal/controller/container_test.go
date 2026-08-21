/*
Copyright 2024 zncdatadev.

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

package controller

import (
	"testing"

	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/zncdatadev/kafka-operator/internal/security"
)

func lookupEnv(envs []corev1.EnvVar, name string) (string, bool) {
	for _, env := range envs {
		if env.Name == name {
			return env.Value, true
		}
	}
	return "", false
}

// The broker JVM must size its heap from the configured memory limit. When the
// env var is missing, kafka-server-start.sh falls back to "-Xmx1G -Xms1G",
// which overruns a 1Gi container limit.
func TestKafkaContainerHeapOptsFromMemoryLimit(t *testing.T) {
	tests := []struct {
		name      string
		resources *commonsv1alpha1.ResourcesSpec
		want      string
		wantSet   bool
	}{
		{
			name: "memory limit sizes the heap",
			resources: &commonsv1alpha1.ResourcesSpec{
				Memory: &commonsv1alpha1.MemoryResource{Limit: resource.MustParse("1Gi")},
			},
			want:    "-Xmx819m",
			wantSet: true,
		},
		{
			name: "no memory spec leaves the heap to the product defaults",
			resources: &commonsv1alpha1.ResourcesSpec{
				CPU: &commonsv1alpha1.CPUResource{Max: resource.MustParse("600m")},
			},
			wantSet: false,
		},
		{
			name:      "no resources at all",
			resources: nil,
			wantSet:   false,
		},
		{
			name: "zero memory limit is not turned into -Xmx0m",
			resources: &commonsv1alpha1.ResourcesSpec{
				Memory: &commonsv1alpha1.MemoryResource{Limit: resource.MustParse("0")},
			},
			wantSet: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			container := NewKafkaContainer(
				"quay.io/zncdatadev/kafka:3.9.0-kubedoop0.0.0-dev",
				corev1.PullIfNotPresent,
				"kafka-znode",
				&security.KafkaSecurity{},
				"default",
				"kafkacluster-sample-broker-default",
				tt.resources,
			)

			got, ok := lookupEnv(container.ContainerEnv(), EnvKafkaHeapOpts)
			if ok != tt.wantSet {
				t.Fatalf("%s set = %v, want %v (value %q)", EnvKafkaHeapOpts, ok, tt.wantSet, got)
			}
			if tt.wantSet && got != tt.want {
				t.Errorf("%s = %q, want %q", EnvKafkaHeapOpts, got, tt.want)
			}
		})
	}
}
