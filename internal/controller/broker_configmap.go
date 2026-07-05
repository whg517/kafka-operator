package controller

import (
	"fmt"
	"maps"
	"path"
	"sort"
	"strings"

	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	opgosecurity "github.com/zncdatadev/operator-go/pkg/security"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kafkav1alpha1 "github.com/zncdatadev/kafka-operator/api/v1alpha1"
	"github.com/zncdatadev/kafka-operator/internal/security"
)

// ComputeProductConfig is the framework ProductConfig hook: it supplies the Kafka default
// config files as the LOWEST merge layer (product < role < role group), so user overrides
// always win and defaults are recomputed every reconcile.
func ComputeProductConfig(_ *kafkav1alpha1.KafkaCluster, _ string, _ string) *commonsv1alpha1.OverridesSpec {
	return &commonsv1alpha1.OverridesSpec{
		ConfigOverrides: map[string]map[string]string{
			kafkav1alpha1.ServerFileName: {
				"zookeeper.connection.timeout.ms": "18000",
				"controlled.shutdown.enable":      "true",
				"log.dirs":                        path.Join(KubedoopDataDir, "topicdata"),
			},
			kafkav1alpha1.SecurityFileName: {
				"networkaddress.cache.ttl":          "30",
				"networkaddress.cache.negative.ttl": "0",
			},
		},
	}
}

// buildConfigMap creates the ConfigMap for a broker role group: server.properties (merged
// defaults + overrides + TLS/Kerberos settings), security.properties, and the
// framework-owned logging entries (log4j.properties and, when Vector is enabled,
// vector.yaml).
func (h *KafkaRoleGroupHandler) buildConfigMap(
	buildCtx *reconciler.RoleGroupBuildContext,
	labels map[string]string,
	kafkaSecurity *security.KafkaSecurity,
	secretProvisioner *opgosecurity.SecretProvisioner,
) (*corev1.ConfigMap, error) {
	data := make(map[string]string)

	data[kafkav1alpha1.ServerFileName] = h.generateServerProperties(buildCtx, kafkaSecurity, secretProvisioner)
	data[kafkav1alpha1.SecurityFileName] = h.generateSecurityProperties(buildCtx)

	// Framework-owned logging config: log4j.properties (from the deep-merged CRD logging
	// spec, with the file appender gated on Vector) and, when Vector is enabled and the CR
	// exposes the aggregator ConfigMap, vector.yaml. h.LoggingContainers is the single
	// declaration that also drives the shared log volume, so config and volume stay in
	// lockstep.
	loggingData, err := reconciler.RenderLoggingConfigMapData(buildCtx, h.LoggingContainers)
	if err != nil {
		return nil, fmt.Errorf("failed to render logging config: %w", err)
	}
	maps.Copy(data, loggingData)

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      buildCtx.ResourceName,
			Namespace: buildCtx.ClusterNamespace,
			Labels:    labels,
		},
		Data: data,
	}, nil
}

// generateServerProperties renders server.properties: the merged config file (product
// defaults + role/role-group overrides, already folded by the framework) plus the
// TLS/Kerberos settings, which are authoritative and therefore applied last.
func (h *KafkaRoleGroupHandler) generateServerProperties(
	buildCtx *reconciler.RoleGroupBuildContext,
	kafkaSecurity *security.KafkaSecurity,
	secretProvisioner *opgosecurity.SecretProvisioner,
) string {
	properties := make(map[string]string)
	if buildCtx.MergedConfig != nil {
		if merged, ok := buildCtx.MergedConfig.ConfigFiles[kafkav1alpha1.ServerFileName]; ok {
			maps.Copy(properties, merged)
		}
	}
	maps.Copy(properties, kafkaSecurity.ConfigSettings(secretProvisioner))
	return toProperties(properties)
}

// generateSecurityProperties renders security.properties (JVM security settings) from the
// merged config file.
func (h *KafkaRoleGroupHandler) generateSecurityProperties(buildCtx *reconciler.RoleGroupBuildContext) string {
	if buildCtx.MergedConfig != nil {
		if merged, ok := buildCtx.MergedConfig.ConfigFiles[kafkav1alpha1.SecurityFileName]; ok {
			return toProperties(merged)
		}
	}
	return ""
}

// toProperties renders a map as a Java properties file with sorted keys, so the output is
// deterministic across reconciles (no spurious ConfigMap updates).
func toProperties(config map[string]string) string {
	keys := make([]string, 0, len(config))
	for k := range config {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(config[k])
		b.WriteString("\n")
	}
	return b.String()
}
