package controller

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	opgoconstant "github.com/zncdatadev/operator-go/pkg/constant"
	"github.com/zncdatadev/operator-go/pkg/listener"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	opgosecurity "github.com/zncdatadev/operator-go/pkg/security"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	kafkav1alpha1 "github.com/zncdatadev/kafka-operator/api/v1alpha1"
	"github.com/zncdatadev/kafka-operator/internal/security"
	"github.com/zncdatadev/kafka-operator/internal/util"
)

// defaultStorageCapacity is the fallback data PVC size when resources.storage is not
// specified, mirroring the pre-framework default so a minimal KafkaCluster still gets
// persistent topic data storage.
const defaultStorageCapacity = "2Gi"

// defaultGracefulShutdownTimeout mirrors the pre-framework default termination grace.
const defaultGracefulShutdownTimeout = 30 * time.Second

// ensureStorageDefault makes sure the merged role group config carries a storage spec, so
// the framework's StatefulSetBuilder.WithStorage builds the data PVC even when the user
// omits resources.storage. Without this the "data" volume mount has no backing PVC and the
// StatefulSet is rejected.
func (h *KafkaRoleGroupHandler) ensureStorageDefault(buildCtx *reconciler.RoleGroupBuildContext) {
	cfg := buildCtx.RoleGroupSpec.Config
	if cfg == nil {
		cfg = &commonsv1alpha1.RoleGroupConfigSpec{}
		buildCtx.RoleGroupSpec.Config = cfg
	}
	if cfg.Resources == nil {
		cfg.Resources = &commonsv1alpha1.ResourcesSpec{}
	}
	switch {
	case cfg.Resources.Storage == nil:
		cfg.Resources.Storage = &commonsv1alpha1.StorageResource{Capacity: resource.MustParse(defaultStorageCapacity)}
	case cfg.Resources.Storage.Capacity.IsZero():
		cfg.Resources.Storage.Capacity = resource.MustParse(defaultStorageCapacity)
	}
}

// customizeStatefulSet applies Kafka specifics to the StatefulSet built by the base
// handler: the start command (with the listener overrides), env, TCP probes, pod
// management policy, affinity and termination grace. Pod identity (ServiceAccount), the
// default pod/container SecurityContext, the config ConfigMap mount, the data PVC, the
// shared Vector log volume and the CSI volumes (TLS/Kerberos/listeners, registered via
// buildCtx.VolumeProviders) are already in place from the framework builder.
func (h *KafkaRoleGroupHandler) customizeStatefulSet(
	sts *appsv1.StatefulSet,
	buildCtx *reconciler.RoleGroupBuildContext,
	cr *kafkav1alpha1.KafkaCluster,
	kafkaSecurity *security.KafkaSecurity,
	secretProvisioner *opgosecurity.SecretProvisioner,
	listenerProvisioner *listener.ListenerProvisioner,
) error {
	podSpec := &sts.Spec.Template.Spec

	if len(podSpec.Containers) == 0 {
		return fmt.Errorf("base handler produced no main container")
	}

	// Brokers are independent; there is no need for ordered rolling starts.
	sts.Spec.PodManagementPolicy = appsv1.ParallelPodManagement

	// The framework renamed the primary container to "kafka"
	// (BaseRoleGroupHandler.MainContainerName) and gave it the framework-managed
	// config/data mounts plus the registered CSI volume mounts, so we only set the
	// command, env and probes here.
	main := &podSpec.Containers[0]
	main.Command = []string{"/bin/bash", "-x", "-euo", "pipefail", "-c"}
	args, err := h.getMainContainerArgs(buildCtx, kafkaSecurity, secretProvisioner, listenerProvisioner)
	if err != nil {
		return err
	}
	main.Args = args
	// User envOverrides (already on the container from the builder) win over our defaults.
	main.Env = append(h.getEnvVars(buildCtx, cr, kafkaSecurity, secretProvisioner), main.Env...)
	main.ReadinessProbe = h.getReadinessProbe(kafkaSecurity)
	main.LivenessProbe = h.getLivenessProbe(kafkaSecurity)

	// Affinity: the generic RoleGroupConfigSpec carries it as a RawExtension the framework
	// does not consume; apply the user's affinity, else the Kafka default (soft
	// anti-affinity between brokers of the same cluster).
	affinity, err := resolveAffinity(buildCtx, cr.Name)
	if err != nil {
		return err
	}
	if podSpec.Affinity == nil {
		podSpec.Affinity = affinity
	}

	// Termination grace from gracefulShutdownTimeout (default 30s).
	if podSpec.TerminationGracePeriodSeconds == nil {
		seconds := int64(resolveGracefulShutdownTimeout(buildCtx).Seconds())
		podSpec.TerminationGracePeriodSeconds = &seconds
	}

	return nil
}

// getMainContainerArgs returns the shell script the main container runs: copy the
// read-only config mount into the writable config dir, resolve the Kerberos realm when
// enabled, then exec the broker with the computed listener overrides so the JVM receives
// SIGTERM directly for graceful shutdown (Vector shutdown ordering is handled by the
// framework's native sidecar).
func (h *KafkaRoleGroupHandler) getMainContainerArgs(
	buildCtx *reconciler.RoleGroupBuildContext,
	kafkaSecurity *security.KafkaSecurity,
	secretProvisioner *opgosecurity.SecretProvisioner,
	listenerProvisioner *listener.ListenerProvisioner,
) ([]string, error) {
	listenerConfig, err := GetKafkaListenerConfig(buildCtx, kafkaSecurity, listenerProvisioner)
	if err != nil {
		return nil, fmt.Errorf("failed to compute listener config: %w", err)
	}

	launch := fmt.Sprintf(
		`exec bin/kafka-server-start.sh %s/%s --override "zookeeper.connect=${ZOOKEEPER}" --override "listeners=%s" --override "advertised.listeners=%s" --override "listener.security.protocol.map=%s"`,
		KubedoopConfigDir, kafkav1alpha1.ServerFileName,
		listenerConfig.ListenersString(),
		listenerConfig.AdvertisedListenersString(),
		listenerConfig.ListenerSecurityProtocolMapString(),
	)

	args := []string{
		// The framework mounts the config ConfigMap read-only at KubedoopConfigDirMount;
		// copy it into the writable config dir (server.properties, security.properties and
		// log4j.properties included — all in that ConfigMap).
		fmt.Sprintf(`CONFIG_DIR_MOUNT=%s
CONFIG_DIR=%s
mkdir --parents ${CONFIG_DIR}
echo copying ${CONFIG_DIR_MOUNT} to ${CONFIG_DIR}
cp -RL ${CONFIG_DIR_MOUNT}* ${CONFIG_DIR}`, opgoconstant.KubedoopConfigDirMount, KubedoopConfigDir),
	}

	if kafkaSecurity.IsKerberosEnabled() {
		kerberosDir := secretProvisioner.MustPath(kafkav1alpha1.KerberosVolumeName)
		args = append(args,
			fmt.Sprintf(`export KERBEROS_REALM=$(grep -oP 'default_realm = \K.*' %s/krb5.conf)`, kerberosDir))

		keytab := fmt.Sprintf("%s/keytab", kerberosDir)
		serviceName := kafkav1alpha1.KerberosServiceName
		brokerAddress := NodeAddressCmd(listenerProvisioner.MustPath(kafkav1alpha1.ListenerBrokerVolumeName))
		bootstrapAddress := NodeAddressCmd(listenerProvisioner.MustPath(kafkav1alpha1.ListenerBootstrapVolumeName))
		launch += fmt.Sprintf(
			` --override "listener.name.client.gssapi.sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true isInitiator=false keyTab=\"%s\" principal=\"%s/%s@$KERBEROS_REALM\";" --override "listener.name.bootstrap.gssapi.sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true isInitiator=false keyTab=\"%s\" principal=\"%s/%s@$KERBEROS_REALM\";"`,
			keytab, serviceName, brokerAddress,
			keytab, serviceName, bootstrapAddress,
		)
	}

	args = append(args, `echo "Starting Kafka"`, launch)
	return []string{strings.Join(args, "\n")}, nil
}

// getEnvVars returns environment variables for the main container.
func (h *KafkaRoleGroupHandler) getEnvVars(
	buildCtx *reconciler.RoleGroupBuildContext,
	cr *kafkav1alpha1.KafkaCluster,
	kafkaSecurity *security.KafkaSecurity,
	secretProvisioner *opgosecurity.SecretProvisioner,
) []corev1.EnvVar {
	envs := []corev1.EnvVar{
		{
			Name: EnvPodName,
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
			},
		},
		{
			Name: EnvNode,
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.hostIP"},
			},
		},
		{
			Name: EnvZookeeperConnections,
			ValueFrom: &corev1.EnvVarSource{
				ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: cr.Spec.ClusterConfig.ZookeeperConfigMapName,
					},
					Key: ZookeeperDiscoveryKey,
				},
			},
		},
		{
			Name:  EnvKafkaLog4jOpts,
			Value: fmt.Sprintf("-Dlog4j.configuration=file:%s/%s", KubedoopConfigDir, kafkav1alpha1.Log4jFileName),
		},
		{
			Name: EnvJvmArgs,
			Value: fmt.Sprintf("-Djava.security.properties=%s/%s -javaagent:%s/jmx/jmx_prometheus_javaagent.jar=%d:%s/jmx/config.yaml",
				KubedoopConfigDir, kafkav1alpha1.SecurityFileName, KubedoopRoot, kafkav1alpha1.MetricsPort, KubedoopRoot),
		},
	}

	if kafkaSecurity.IsKerberosEnabled() {
		krb5Conf := fmt.Sprintf("%s/krb5.conf", secretProvisioner.MustPath(kafkav1alpha1.KerberosVolumeName))
		envs = append(envs,
			corev1.EnvVar{Name: "KRB5_CONFIG", Value: krb5Conf},
			corev1.EnvVar{Name: "KAFKA_OPTS", Value: fmt.Sprintf("-Djava.security.krb5.conf=%s", krb5Conf)},
		)
	}

	// Heap limit from memory resources (80% of the limit).
	roleGroupConfig := buildCtx.RoleGroupSpec.GetConfig()
	if roleGroupConfig != nil && roleGroupConfig.Resources != nil && roleGroupConfig.Resources.Memory != nil {
		memoryLimit := roleGroupConfig.Resources.Memory.Limit
		heap := int(util.QuantityToMB(memoryLimit) * 0.8)
		if heap > 0 {
			envs = append(envs, corev1.EnvVar{
				Name:  EnvKafkaHeapOpts,
				Value: fmt.Sprintf("-Xmx%dm", heap),
			})
		}
	}

	return envs
}

// getLivenessProbe returns the liveness probe (TCP on the client port).
func (h *KafkaRoleGroupHandler) getLivenessProbe(kafkaSecurity *security.KafkaSecurity) *corev1.Probe {
	return &corev1.Probe{
		FailureThreshold:    6,
		InitialDelaySeconds: 20,
		PeriodSeconds:       30,
		SuccessThreshold:    1,
		TimeoutSeconds:      5,
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString(kafkaSecurity.ClientPortName())},
		},
	}
}

// getReadinessProbe returns the readiness probe (TCP on the client port).
func (h *KafkaRoleGroupHandler) getReadinessProbe(kafkaSecurity *security.KafkaSecurity) *corev1.Probe {
	return &corev1.Probe{
		FailureThreshold:    3,
		InitialDelaySeconds: 20,
		PeriodSeconds:       30,
		SuccessThreshold:    1,
		TimeoutSeconds:      1,
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString(kafkaSecurity.ClientPortName())},
		},
	}
}

// resolveAffinity returns the user-provided affinity from the role group config
// (RawExtension) when set, else the Kafka default: soft anti-affinity between brokers of
// the same cluster.
func resolveAffinity(buildCtx *reconciler.RoleGroupBuildContext, clusterName string) (*corev1.Affinity, error) {
	if cfg := buildCtx.RoleGroupSpec.GetConfig(); cfg != nil && cfg.Affinity != nil && len(cfg.Affinity.Raw) > 0 {
		affinity := &corev1.Affinity{}
		if err := json.Unmarshal(cfg.Affinity.Raw, affinity); err != nil {
			return nil, fmt.Errorf("failed to unmarshal affinity: %w", err)
		}
		return affinity, nil
	}
	return defaultAffinity(clusterName), nil
}

// defaultAffinity is the Kafka default: prefer spreading brokers of the same cluster
// across nodes.
func defaultAffinity(clusterName string) *corev1.Affinity {
	return &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
				{
					Weight: 70,
					PodAffinityTerm: corev1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								LabelKubernetesInstance:       clusterName,
								"app.kubernetes.io/component": kafkav1alpha1.BrokerRoleName,
							},
						},
						TopologyKey: corev1.LabelHostname,
					},
				},
			},
		},
	}
}

// resolveGracefulShutdownTimeout parses gracefulShutdownTimeout from the role group
// config, defaulting to 30s.
func resolveGracefulShutdownTimeout(buildCtx *reconciler.RoleGroupBuildContext) time.Duration {
	cfg := buildCtx.RoleGroupSpec.GetConfig()
	if cfg == nil || cfg.GracefulShutdownTimeout == "" {
		return defaultGracefulShutdownTimeout
	}
	d, err := time.ParseDuration(cfg.GracefulShutdownTimeout)
	if err != nil || d <= 0 {
		return defaultGracefulShutdownTimeout
	}
	return d
}
