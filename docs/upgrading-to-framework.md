# Upgrading from a pre-framework (operator-go v0.12.6) operator

The framework rewrite keeps every resource NAME stable (`<cluster>-broker-<group>` for the
StatefulSet/ConfigMap/Services, `data-*` for the topic-data PVCs, `<cluster>` /
`<cluster>-nodeport` for the discovery ConfigMaps), but three **immutable** StatefulSet
fields changed, so an in-place operator upgrade cannot update an existing StatefulSet:

| Field | pre-framework | framework |
| ------- | --------------- | ----------- |
| `spec.selector` | descriptive `app.kubernetes.io/*` labels | `kafka.kubedoop.dev/{cluster,role,role-group}` identity labels |
| `spec.serviceName` | `<cluster>-broker-<group>` | `<cluster>-broker-<group>-headless` |
| `spec.volumeClaimTemplates` | `listener-bootstrap` + `data` | `data` only |

The role-group Service also changes from headless to ClusterIP (immutable `clusterIP`), and
the per-cluster ServiceAccount is renamed from `<cluster>` to the framework-derived
`kafkacluster-<cluster>` (`<lowercased kind>-<cluster>`). The rename happens automatically
on the next reconcile (a rolling pod restart); the old ServiceAccount is left behind for
you to delete.

## Migration steps (per KafkaCluster, brokers restart once)

1. Scale down / stop the old operator.
2. For every broker role group:

   ```bash
   kubectl delete statefulset <cluster>-broker-<group>          # data PVCs are retained
   kubectl delete service     <cluster>-broker-<group>          # old headless service
   kubectl delete pvc -l 'app.kubernetes.io/instance=<cluster>' \
       --field-selector 'metadata.name notin ()' --dry-run=client -o name \
       | grep '^persistentvolumeclaim/listener-bootstrap-' | xargs -r kubectl delete  # orphaned bootstrap PVCs
   kubectl delete serviceaccount <cluster>                       # old per-cluster SA
   ```

3. Deploy the new operator. It recreates the StatefulSet with the same name; pods reattach
   to the existing `data-*` PVCs, so topic data is preserved. Brokers restart once and their
   INTERNAL advertised address moves to the new headless service DNS
   (`$POD.<cluster>-broker-<group>-headless.<ns>.svc`); the cluster re-forms after the
   rolling start.

Client-facing contracts (discovery ConfigMap name/key, bootstrap Listener name, ports) are
unchanged, so clients need no action.
