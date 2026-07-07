package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/zncdatadev/operator-go/pkg/builder"
	"github.com/zncdatadev/operator-go/pkg/listener"
	"github.com/zncdatadev/operator-go/pkg/productlogging"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	opgosecurity "github.com/zncdatadev/operator-go/pkg/security"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	kafkav1alpha1 "github.com/zncdatadev/kafka-operator/api/v1alpha1"
	"github.com/zncdatadev/kafka-operator/internal/security"
	"github.com/zncdatadev/kafka-operator/internal/util/version"
)

var logger = ctrl.Log.WithName("kafka-handler")

// parseSecretLifetime parses a certificate lifetime expression. On top of Go duration
// syntax it supports the day suffix documented in the CRD ("1d", "7d", "30d") —
// secret-operator itself parses the annotation with Go duration syntax only.
func parseSecretLifetime(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if strings.HasSuffix(s, "d") {
		days, err := strconv.ParseFloat(strings.TrimSuffix(s, "d"), 64)
		if err != nil {
			return 0, fmt.Errorf("invalid day expression %q: %w", s, err)
		}
		return time.Duration(days * 24 * float64(time.Hour)), nil
	}
	return time.ParseDuration(s)
}

// RBAC for the GenericReconciler-driven KafkaCluster controller: the CR itself, the role
// group resources the framework applies (ConfigMap/Services/StatefulSet/PDB/SA), the
// bootstrap Listener CRs (ExtraResources), pods for health checks, and events.
//
// +kubebuilder:rbac:groups=kafka.kubedoop.dev,resources=kafkaclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kafka.kubedoop.dev,resources=kafkaclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=kafka.kubedoop.dev,resources=kafkaclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps;services;serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=listeners.kubedoop.dev,resources=listeners,verbs=get;list;watch;create;update;patch;delete

// LabelDomain is the product domain used for identity (selector) labels:
// kafka.kubedoop.dev/{cluster,role,role-group}. The product-domain prefix guarantees
// these selectors never match another product's pods.
const LabelDomain = "kafka.kubedoop.dev"

// LabelListenerBootstrap marks the per-role-group bootstrap Listener so discovery can
// find all bootstrap listeners of a cluster with a label selector.
const (
	LabelListenerBootstrap = "app.kubernetes.io/listener-bootstrap"
	LabelValueTrue         = "true"
)

// kafkaServerLogging is the single source of truth for the Kafka main container's logging.
// It drives both BaseRoleGroupHandler.LoggingContainers (the framework's shared Vector log
// volume producer/consumer wiring) and the log4j.properties + vector.yaml rendered into the
// role group ConfigMap. Kafka 3.x logs via reload4j, so the log4j 1.x generator is used.
// The framework derives the rolling file path from the container name and framework
// ("/kubedoop/log/kafka/kafka.log4j.xml", XMLLayout), which the Vector sidecar edge-parses.
var kafkaServerLogging = productlogging.ContainerLogging{
	Container: kafkav1alpha1.KafkaContainerName,
	Framework: productlogging.LoggingFrameworkLog4j,
	Pattern:   "[%d] %p %m (%c)%n",
}

// KafkaRoleGroupHandler builds the resources for a Kafka broker role group.
//
// It embeds reconciler.BaseRoleGroupHandler to inherit the framework's canonical resource
// construction (labels, headless/client Services, builder-built StatefulSet with the data
// PVC, PodDisruptionBudget, sidecar injection, CSI volume injection via VolumeProviders),
// then customizes the returned StatefulSet and ConfigMap with Kafka specifics (start
// command with listener overrides, TCP probes, server.properties). The per-role-group
// bootstrap Listener CR is shipped through RoleGroupResources.ExtraResources so the
// framework applies it before the StatefulSet (pods mount a CSI volume referencing it).
type KafkaRoleGroupHandler struct {
	reconciler.BaseRoleGroupHandler[*kafkav1alpha1.KafkaCluster]

	// mu serializes BuildResources: the per-CR inputs (Image, RoleContainerPorts,
	// RoleServicePorts) are handler-wide fields on a shared instance, so concurrent
	// reconciles (if MaxConcurrentReconciles is ever raised) would race and leak
	// configuration between clusters. Serializing keeps the set-then-build sequence
	// atomic per reconcile.
	mu sync.Mutex
}

var _ reconciler.RoleGroupHandler[*kafkav1alpha1.KafkaCluster] = &KafkaRoleGroupHandler{}

// NewKafkaRoleGroupHandler creates a handler with the framework-level options that are
// constant across reconciliations. Per-CR options (image, ports) are set in BuildResources.
func NewKafkaRoleGroupHandler(scheme *runtime.Scheme) *KafkaRoleGroupHandler {
	h := &KafkaRoleGroupHandler{}
	h.Scheme = scheme
	h.ImagePullPolicy = kafkav1alpha1.ImagePullPolicy
	h.RoleImages = map[string]string{}
	h.RoleContainerPorts = map[string][]corev1.ContainerPort{}
	h.RoleServicePorts = map[string][]corev1.ServicePort{}
	// app.kubernetes.io/name identifies the product on every resource/pod; the framework's
	// canonical labels alone (instance + component + managed-by) are not product-unique.
	h.ExtraLabels = map[string]string{
		LabelKubernetesName: kafkav1alpha1.DefaultProductName,
	}
	h.ExtraAnnotations = map[string]string{}
	// Brokers must resolve each other before readiness, and topic data must be persistent.
	h.PublishNotReadyAddresses = true
	h.StorageMountPath = KubedoopDataDir
	// Rename the primary container to "kafka" and declare it as the logging container. The
	// framework renames the container before injecting the shared Vector log volume, so the
	// producer mounts it on "kafka".
	h.MainContainerName = kafkav1alpha1.KafkaContainerName
	h.LoggingContainers = []productlogging.ContainerLogging{kafkaServerLogging}
	// Product-owned identity labels drive all resource selectors (decoupled from the
	// descriptive app.kubernetes.io/* labels).
	h.LabelDomain = LabelDomain
	return h
}

// BuildResources builds all Kubernetes resources for a Kafka broker role group.
func (h *KafkaRoleGroupHandler) BuildResources(
	ctx context.Context,
	k8sClient ctrlclient.Client,
	cr *kafkav1alpha1.KafkaCluster,
	buildCtx *reconciler.RoleGroupBuildContext,
) (*reconciler.RoleGroupResources, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if buildCtx.RoleName != kafkav1alpha1.BrokerRoleName {
		return nil, fmt.Errorf("unsupported role: %s", buildCtx.RoleName)
	}
	if cr.Spec.ClusterConfig == nil || cr.Spec.ClusterConfig.ZookeeperConfigMapName == "" {
		return nil, fmt.Errorf("spec.clusterConfig.zookeeperConfigMapName is required")
	}

	// Resolve Kafka-specific inputs.
	kafkaSecurity := security.NewKafkaSecurity(cr)
	brokerCfg := resolveBrokerConfig(cr, buildCtx.RoleGroupName)
	secretProvisioner := h.buildSecretProvisioner(kafkaSecurity, brokerCfg)
	bootstrapListenerName := BootstrapListenerName(buildCtx.ResourceName)
	listenerProvisioner := h.buildListenerProvisioner(brokerCfg, bootstrapListenerName)
	image := h.resolveImage(cr)

	// Configure the per-CR base inputs (each field is set unconditionally on every call:
	// the handler is shared across CRs, so a skipped set would leak the previous CR's value).
	h.Image = image
	h.ImagePullPolicy = kafkav1alpha1.ImagePullPolicy
	if cr.Spec.Image != nil && cr.Spec.Image.PullPolicy != nil {
		h.ImagePullPolicy = *cr.Spec.Image.PullPolicy
	}
	h.SetRoleContainerPorts(kafkav1alpha1.BrokerRoleName, KafkaContainerPorts(kafkaSecurity))
	h.SetRoleServicePorts(kafkav1alpha1.BrokerRoleName, kafkaServicePorts(kafkaSecurity))
	// Ensure the Kafka resource defaults (storage/CPU/memory) for anything the user omitted.
	h.ensureResourceDefaults(buildCtx)

	// The framework's GenericReconciler already constructs the Vector sidecar pointed at
	// this role group's ConfigMap; we only need to set the product image on the registered
	// sidecars.
	if err := buildCtx.SidecarManager.SetProductImage(image, h.ImagePullPolicy); err != nil {
		return nil, fmt.Errorf("failed to set product image on sidecars: %w", err)
	}

	// Hand the CSI volumes (TLS keystores, Kerberos keytab, listener addresses) to the
	// framework so base.BuildResources() injects them into the pod and the main container.
	// VolumeProviders lives on the build context (rebuilt each reconcile), so registrations
	// never accumulate across reconciles or leak across CRs.
	buildCtx.VolumeProviders = append(buildCtx.VolumeProviders, secretProvisioner, listenerProvisioner)

	// Let the framework build the skeleton: canonical labels, headless Service (with
	// PublishNotReadyAddresses), client Service, StatefulSet (data PVC + injected
	// sidecars + CSI volumes), and PodDisruptionBudget.
	res, err := h.BaseRoleGroupHandler.BuildResources(ctx, k8sClient, cr, buildCtx)
	if err != nil {
		return nil, fmt.Errorf("base build failed: %w", err)
	}

	// Customize the StatefulSet with Kafka specifics.
	if err := h.customizeStatefulSet(res.StatefulSet, buildCtx, cr, kafkaSecurity, secretProvisioner, listenerProvisioner); err != nil {
		return nil, err
	}

	// Replace the ConfigMap with computed Kafka config (server.properties,
	// security.properties, log4j.properties, vector.yaml). Reuse the framework labels base
	// put on the StatefulSet.
	cm, err := h.buildConfigMap(buildCtx, res.StatefulSet.Labels, kafkaSecurity, secretProvisioner)
	if err != nil {
		return nil, fmt.Errorf("failed to build configmap: %w", err)
	}
	res.ConfigMap = cm

	// Metrics Service (headless with Prometheus scrape annotations). Its selector uses the
	// identity labels, consistent with the other role-group resources.
	res.MetricsService = builder.NewMetricsServiceBuilder(
		buildCtx.ResourceName,
		buildCtx.ClusterNamespace,
		kafkav1alpha1.MetricsPort,
		res.StatefulSet.Labels,
	).WithSelector(h.SelectorLabels(buildCtx)).
		// Target the container port by name so renumbering never breaks the Service
		// (pre-framework parity).
		WithTargetPortName(kafkav1alpha1.MetricsPortName).Build()

	// The per-role-group bootstrap Listener gives clients a stable bootstrap address. It is
	// shipped as an extra resource so the framework applies it BEFORE the StatefulSet: the
	// pods' listener-bootstrap CSI volume references it by name, and would otherwise hang in
	// ContainerCreating.
	res.ExtraResources = append(res.ExtraResources,
		h.buildBootstrapListener(bootstrapListenerName, buildCtx, res.StatefulSet.Labels, brokerCfg, kafkaSecurity))

	return res, nil
}

// buildSecretProvisioner declares all CSI secret volumes needed by the broker based on the
// security configuration. TLS keystore volumes are scoped to the listener volumes (plus
// pod and node) so certificates carry the listener addresses as SANs.
func (h *KafkaRoleGroupHandler) buildSecretProvisioner(
	kafkaSecurity *security.KafkaSecurity,
	brokerCfg *brokerConfig,
) *opgosecurity.SecretProvisioner {
	provisioner := opgosecurity.NewSecretProvisioner()

	listenerScopes := strings.Join([]string{
		string(opgosecurity.ListenerVolumeScope) + "=" + kafkav1alpha1.ListenerBrokerVolumeName,
		string(opgosecurity.ListenerVolumeScope) + "=" + kafkav1alpha1.ListenerBootstrapVolumeName,
	}, opgosecurity.CommonDelimiter)

	tlsScope := strings.Join([]string{
		listenerScopes,
		string(opgosecurity.PodScope),
		string(opgosecurity.NodeScope),
	}, opgosecurity.CommonDelimiter)

	registerTLS := func(volumeName, secretClass string) {
		reg := opgosecurity.TLS(volumeName, secretClass).WithScope(tlsScope)
		if kafkaSecurity.SSLStorePassword != "" {
			reg.WithPassword(kafkaSecurity.SSLStorePassword)
		}
		if brokerCfg.RequestedSecretLifeTime != "" {
			// The CRD documents day expressions ("7d", "30d"); secret-operator parses the
			// annotation with Go duration syntax, so convert before handing it to the
			// framework. Invalid expressions are skipped (secret-operator would reject the
			// whole volume otherwise, wedging the pod in Pending).
			if lifetime, err := parseSecretLifetime(brokerCfg.RequestedSecretLifeTime); err == nil {
				reg.WithCertLifetime(lifetime)
			} else {
				logger.V(0).Info("ignoring invalid requestedSecretLifeTime",
					"value", brokerCfg.RequestedSecretLifeTime, "error", err.Error())
			}
		}
		provisioner.Register(reg)
	}

	if serverClass := kafkaSecurity.TlsServerSecretClass(); serverClass != "" {
		registerTLS(kafkav1alpha1.TLSKeystoreServerVolumeName, serverClass)
	}

	if internalClass := kafkaSecurity.TlsInternalSecretClass(); internalClass != "" {
		registerTLS(kafkav1alpha1.TLSKeystoreInternalVolumeName, internalClass)
	}

	if kafkaSecurity.IsKerberosEnabled() {
		provisioner.Register(opgosecurity.KerberosVolume(
			kafkav1alpha1.KerberosVolumeName,
			kafkaSecurity.KerberosSecretClass(),
			kafkav1alpha1.KerberosServiceName,
		).WithScope(listenerScopes))
	}

	return provisioner
}

// buildListenerProvisioner declares the listener CSI volumes: the per-broker listener
// (provisioned from the role group's broker listener class) and the bootstrap listener
// (referencing the pre-created bootstrap Listener by name).
func (h *KafkaRoleGroupHandler) buildListenerProvisioner(
	brokerCfg *brokerConfig,
	bootstrapListenerName string,
) *listener.ListenerProvisioner {
	return listener.NewProvisioner().RegisterVolume(
		listener.NewVolume(kafkav1alpha1.ListenerBrokerVolumeName, listener.ListenerClass(brokerCfg.BrokerListenerClass)),
		// The bootstrap volume references the pre-created bootstrap Listener by name; the
		// class lives on the Listener CR itself (the registration omits the class
		// annotation entirely for by-name references).
		listener.NewVolume(kafkav1alpha1.ListenerBootstrapVolumeName, "").
			WithListenerName(bootstrapListenerName),
	)
}

// buildBootstrapListener builds the per-role-group bootstrap Listener CR.
func (h *KafkaRoleGroupHandler) buildBootstrapListener(
	name string,
	buildCtx *reconciler.RoleGroupBuildContext,
	labels map[string]string,
	brokerCfg *brokerConfig,
	kafkaSecurity *security.KafkaSecurity,
) ctrlclient.Object {
	return NewBootstrapListener(name, buildCtx.ClusterNamespace, labels, brokerCfg.BootstrapListenerClass, kafkaSecurity)
}

// resolveImage constructs the container image string from the CR spec. Kubedoop product
// images are tagged "<productVersion>-kubedoop<kubedoopVersion>"; when the CR does not pin
// a kubedoop version, the operator's own build version is used (dev operator -> dev image),
// matching the pre-framework behavior.
func (h *KafkaRoleGroupHandler) resolveImage(cr *kafkav1alpha1.KafkaCluster) string {
	repo := kafkav1alpha1.DefaultRepository
	productVersion := kafkav1alpha1.DefaultProductVersion
	kubedoopVersion := version.BuildVersion

	if img := cr.Spec.Image; img != nil {
		if img.Custom != "" {
			return img.Custom
		}
		if img.Repo != "" {
			repo = img.Repo
		}
		if img.ProductVersion != "" {
			productVersion = img.ProductVersion
		}
		if img.KubedoopVersion != "" {
			kubedoopVersion = img.KubedoopVersion
		}
	}

	return fmt.Sprintf("%s/%s:%s-kubedoop%s",
		repo, kafkav1alpha1.DefaultProductName, productVersion, kubedoopVersion)
}

// brokerConfig carries the Kafka-specific role group settings that live outside the
// framework's generic RoleGroupConfigSpec (and therefore outside MergedConfig): the
// listener classes and the requested certificate lifetime. Resolution order is
// role group config > role config > default.
type brokerConfig struct {
	BrokerListenerClass     string
	BootstrapListenerClass  string
	RequestedSecretLifeTime string
}

const (
	defaultListenerClass           = "cluster-internal"
	defaultRequestedSecretLifeTime = "1d"
)

// resolveBrokerConfig resolves the Kafka-specific broker settings for a role group.
func resolveBrokerConfig(cr *kafkav1alpha1.KafkaCluster, roleGroupName string) *brokerConfig {
	cfg := &brokerConfig{
		BrokerListenerClass:     defaultListenerClass,
		BootstrapListenerClass:  defaultListenerClass,
		RequestedSecretLifeTime: defaultRequestedSecretLifeTime,
	}

	apply := func(spec *kafkav1alpha1.BrokersConfigSpec) {
		if spec == nil {
			return
		}
		if spec.BrokerListenerClass != "" {
			cfg.BrokerListenerClass = spec.BrokerListenerClass
		}
		if spec.BootstrapListenerClass != "" {
			cfg.BootstrapListenerClass = spec.BootstrapListenerClass
		}
		if spec.RequestedSecretLifeTime != "" {
			cfg.RequestedSecretLifeTime = spec.RequestedSecretLifeTime
		}
	}

	if cr.Spec.Brokers == nil {
		return cfg
	}
	// Role-level config first, then the role group's own config on top.
	apply(cr.Spec.Brokers.Config)
	if rg, ok := cr.Spec.Brokers.RoleGroups[roleGroupName]; ok && rg != nil {
		apply(rg.Config)
	}
	return cfg
}
