package controller

import (
	"context"
	"fmt"
	"strings"

	listenerv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/listeners/v1alpha1"
	opcommon "github.com/zncdatadev/operator-go/pkg/common"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	kafkav1alpha1 "github.com/zncdatadev/kafka-operator/api/v1alpha1"
	"github.com/zncdatadev/kafka-operator/internal/security"
)

// KafkaDiscoveryKey is the key of the bootstrap servers list in the discovery ConfigMaps.
const KafkaDiscoveryKey = "KAFKA"

// DiscoveryExtension is a cluster-scope reconciliation extension that publishes the Kafka
// bootstrap addresses to clients via discovery ConfigMaps ("<cluster>" and
// "<cluster>-nodeport"), aggregated from the ingress addresses of all bootstrap Listeners
// of the cluster.
//
// It runs in PostReconcile: the bootstrap Listener CRs are created during role group
// reconciliation (RoleGroupResources.ExtraResources), and their status carries the ingress
// addresses only after listener-operator has processed them.
type DiscoveryExtension struct {
	scheme *runtime.Scheme
}

var _ opcommon.ClusterExtension[opcommon.ClusterInterface] = &DiscoveryExtension{}

// NewDiscoveryExtension creates a new DiscoveryExtension.
func NewDiscoveryExtension(scheme *runtime.Scheme) *DiscoveryExtension {
	return &DiscoveryExtension{scheme: scheme}
}

// Name implements opcommon.Extension.
func (e *DiscoveryExtension) Name() string { return "kafka-discovery" }

// PreReconcile is a no-op.
func (e *DiscoveryExtension) PreReconcile(_ context.Context, _ client.Client, _ opcommon.ClusterInterface) error {
	return nil
}

// PostReconcile aggregates the bootstrap listener addresses and writes the discovery
// ConfigMaps.
func (e *DiscoveryExtension) PostReconcile(ctx context.Context, c client.Client, cr opcommon.ClusterInterface) error {
	kafkaCluster, ok := cr.(*kafkav1alpha1.KafkaCluster)
	if !ok {
		// Not a KafkaCluster; the global registry is shared, so just skip.
		return nil
	}
	return e.ensureDiscoveryConfigMaps(ctx, c, kafkaCluster)
}

// OnReconcileError is a no-op.
func (e *DiscoveryExtension) OnReconcileError(_ context.Context, _ client.Client, _ opcommon.ClusterInterface, _ error) error {
	return nil
}

func (e *DiscoveryExtension) ensureDiscoveryConfigMaps(ctx context.Context, c client.Client, cr *kafkav1alpha1.KafkaCluster) error {
	kafkaSecurity := security.NewKafkaSecurity(cr)

	// Clients bootstrap against the client port; with Kerberos they must use the dedicated
	// BOOTSTRAP listener port instead.
	portName := kafkaSecurity.ClientPortName()
	if kafkaSecurity.IsKerberosEnabled() {
		portName = kafkaSecurity.BootstrapPortName()
	}

	// Select only this cluster's bootstrap Listeners: the bootstrap marker narrows to
	// bootstrap listeners, the product identity label prevents picking up another
	// cluster's (or product's) listeners in the same namespace.
	listenerList := &listenerv1alpha1.ListenerList{}
	if err := c.List(ctx, listenerList,
		client.InNamespace(cr.Namespace),
		client.MatchingLabels{
			LabelListenerBootstrap:                  LabelValueTrue,
			reconciler.ClusterLabelKey(LabelDomain): cr.Name,
		},
	); err != nil {
		return fmt.Errorf("failed to list bootstrap listeners: %w", err)
	}

	// Eagerly delete bootstrap Listeners whose role group no longer exists in the spec:
	// the framework cleaner cannot discover ExtraResources (operator-go#516), and a stale
	// Listener would keep serving dead bootstrap addresses through discovery.
	expected := expectedBootstrapListeners(cr)
	kept := listenerList.Items[:0]
	for i := range listenerList.Items {
		l := &listenerList.Items[i]
		if _, ok := expected[l.Name]; ok {
			kept = append(kept, *l)
			continue
		}
		if err := c.Delete(ctx, l); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete orphaned bootstrap listener %s/%s: %w", l.Namespace, l.Name, err)
		}
		log.FromContext(ctx).Info("deleted orphaned bootstrap listener", "listener", l.Name)
	}
	listenerList.Items = kept

	bootstrapServers := makeBootstrapServers(ctx, listenerList, portName)

	// The framework helper owns the ensure semantics (CreateOrUpdate + controller owner
	// reference + canonical labels); the product only computes the data.
	for _, name := range []string{cr.Name, cr.Name + "-nodeport"} {
		if err := reconciler.EnsureDiscoveryConfigMap(ctx, c, e.scheme, cr, name,
			map[string]string{KafkaDiscoveryKey: bootstrapServers},
			reconciler.WithDiscoveryProductName(kafkav1alpha1.DefaultProductName),
			reconciler.WithDiscoveryExtraLabels(map[string]string{
				reconciler.ClusterLabelKey(LabelDomain): cr.Name,
			}),
		); err != nil {
			return fmt.Errorf("failed to ensure discovery configmap %s/%s: %w", cr.Namespace, name, err)
		}
	}

	log.FromContext(ctx).V(1).Info("ensured discovery configmaps",
		"cluster", cr.Name, "bootstrapServers", bootstrapServers)
	return nil
}

// expectedBootstrapListeners returns the set of bootstrap Listener names the current spec
// produces (one per broker role group).
func expectedBootstrapListeners(cr *kafkav1alpha1.KafkaCluster) map[string]struct{} {
	expected := map[string]struct{}{}
	if cr.Spec.Brokers == nil {
		return expected
	}
	for group := range cr.Spec.Brokers.RoleGroups {
		name := BootstrapListenerName(reconciler.RoleGroupResourceName(cr.Name, kafkav1alpha1.BrokerRoleName, group))
		expected[name] = struct{}{}
	}
	return expected
}

// makeBootstrapServers renders the comma-separated host:port bootstrap list from the
// listeners' ingress addresses. Listeners missing the requested port name are skipped (and
// logged) instead of failing: one stale Listener (e.g. created before a TLS toggle renamed
// the port) must not wedge the whole cluster's reconciliation.
func makeBootstrapServers(ctx context.Context, listenerList *listenerv1alpha1.ListenerList, portName string) string {
	var servers []string
	for _, l := range listenerList.Items {
		for _, addr := range l.Status.IngressAddresses {
			port, ok := addr.Ports[portName]
			if !ok {
				log.FromContext(ctx).Info("skipping bootstrap listener without the expected port",
					"listener", l.Name, "port", portName)
				continue
			}
			servers = append(servers, fmt.Sprintf("%s:%d", addr.Address, port))
		}
	}
	return strings.Join(servers, ",")
}
