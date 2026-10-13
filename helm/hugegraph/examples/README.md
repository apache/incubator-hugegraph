# HugeGraph deployment examples

These files are examples, not automatically installed resources. Replace registry,
node names, paths, release/namespace and credentials for your cluster before use.
Commands below are deployment instructions; authoring them does not deploy anything.

## Storage

The default topology needs **seven volumes**: 3 × 10 GiB for PD, 3 × 50 GiB for
Store, and 1 × 1 GiB for Hubble. With a default dynamic StorageClass, ordinary
`helm install` requests them automatically. For a named dynamic StorageClass:

```bash
helm install hugegraph ./helm/hugegraph -n hugegraph --create-namespace \
  --set pd.storage.storageClassName=my-csi \
  --set store.storage.storageClassName=my-csi \
  --set hubble.persistence.storageClassName=my-csi
```

An empty chart `storageClassName` inherits the cluster default; `"-"` renders
an explicit empty Kubernetes StorageClass and disables default-class assignment.
Neither creates a provisioner. PVCs remain Pending without matching PVs.

### Static local volumes

[local-pvs.yaml](local-pvs.yaml) contains a `kubernetes.io/no-provisioner`
StorageClass with `WaitForFirstConsumer` and all seven prebound PVs. It assumes
release/namespace `hugegraph` and hostname labels `worker-1`, `worker-2`, `worker-3`.
Change both `claimRef` and nodes if those names differ. StatefulSet claim names
are `<volume-name>-<fullname>-<component>-<ordinal>`; Hubble uses
`<fullname>-hubble-data`.

On each indicated host, create these directories on disks with sufficient free
space and make them writable by the UID/GID of the chosen images (published
images currently run as root). Existing unrelated data must not be reused.

| Node | Directories | Required capacities |
|---|---|---|
| worker-1 | `/var/lib/hugegraph/pd-0`, `/var/lib/hugegraph/store-0`, `/var/lib/hugegraph/hubble` | 10 GiB + 50 GiB + 1 GiB |
| worker-2 | `/var/lib/hugegraph/pd-1`, `/var/lib/hugegraph/store-1` | 10 GiB + 50 GiB |
| worker-3 | `/var/lib/hugegraph/pd-2`, `/var/lib/hugegraph/store-2` | 10 GiB + 50 GiB |

PV capacity is a binding declaration, not a directory quota; arrange filesystem
capacity separately. Check labels with `kubectl get nodes -L kubernetes.io/hostname`,
then apply the reviewed resources:

```bash
kubectl create namespace hugegraph
kubectl apply -f helm/hugegraph/examples/local-pvs.yaml
helm install hugegraph ./helm/hugegraph -n hugegraph \
  -f helm/hugegraph/examples/values-local-storage.yaml --wait --timeout 15m
```

`WaitForFirstConsumer` may leave a PVC Pending until a Pod can be scheduled;
local PV node affinity and workload affinity/nodeSelector must allow the same
node. Local volumes survive Pod replacement on that node, but cannot move their
files to another node on failure. `Retain` keeps data on claim deletion;
recovery/reuse requires operator inspection.

For a fresh temporary installation, `--set persistence.enabled=false` uses
`emptyDir`. Component `pd.storage.enabled`, `store.storage.enabled` and
`hubble.persistence.enabled` override the global choice. A failed PVC never
falls back to temporary storage. Changing an initialized cluster's storage mode
requires a separate data migration/recovery procedure.

## Internal images

[values-internal-registry.yaml](values-internal-registry.yaml) mirrors the default
four images: `hugegraph/pd:latest`, `hugegraph/store:latest`,
`hugegraph/hugegraph:latest`, and `hugegraph/hubble:latest`. Tags follow the chart's
current version policy; pin your own published tags/digests.

`global.imageRegistry` prefixes repositories without an explicit registry.
A repository such as `other-registry.example.com/team/pd` overrides that prefix.
Registry credentials remain `imagePullSecrets`.

The PD wait init container reuses the Store image and Helm test reuses the
Server image. Both already include `curl`; the chart selects a shell command
rather than the component entrypoint. No extra `curlimages/curl` image or runtime
package installation is needed by default. `store.waitImage` and `server.waitImage`
remain explicit overrides for custom images. Store wait overrides need
`/bin/sh`, `curl`, `tr`, `date` and `sleep`; Server test overrides additionally
need `base64`, `getent`, `awk`, `sort`, `grep` and `tail`. Reused component images
also inherit the component pull policy. Older values may retain a curl image
under `--reuse-values`; clear those two fields to restore inheritance.

```bash
helm install hugegraph ./helm/hugegraph -n hugegraph --create-namespace \
  -f helm/hugegraph/examples/values-internal-registry.yaml --wait --timeout 15m
```

## GitOps credentials

Use [values-gitops-fixed.yaml](values-gitops-fixed.yaml) with encrypted values,
or [values-gitops-existing-secrets.yaml](values-gitops-existing-secrets.yaml) with
a secret controller. Replace sample credentials before use. For the latter,
provision these three Secret data keys before the workloads start:

| Secret | Key | Purpose |
|---|---|---|
| `hugegraph-pd-credentials` | `secret-key` | Shared PD REST Basic-auth password |
| `hugegraph-admin-credentials` | `password` | Initial Server admin password |
| `hugegraph-token-credentials` | `secret` | Shared JWT signing key, at least 32 bytes |

The admin password must be printable ASCII without spaces, colons or backslashes.
PD credentials must be printable ASCII without surrounding whitespace or
backslashes. `existingSecret` takes priority over fixed values and is not managed
by the chart. Ordinary Helm installations generate/reuse credentials; repeated
offline renders cannot look up live generated Secrets and otherwise generate new
random values. Changing the admin Secret alone does not change an already-created
account; follow the README's password rotation procedure.

## Entry points and UI

[values-http-ingress.yaml](values-http-ingress.yaml) uses an already installed
nginx controller for hostless HTTP Server Ingress. Both modes allow HTTP;
certificates, hostnames and acknowledgement fields are optional.
NodePort needs reachable node IPs and firewall permissions; query the actual port
with `kubectl -n hugegraph get svc`, then use `http://<node-ip>:<port>`.
ClusterIP stays inside the cluster; use a gateway or port-forward for external
access. LoadBalancer needs your infrastructure's controller.

[values-api-only.yaml](values-api-only.yaml) disables Hubble. Keep auth for an
authenticated API; disable `server.auth.enabled` as well for an unauthenticated
API. Current Hubble login still needs Server auth. Strict mode defaults to internal
Services and NetworkPolicy, but explicit parameters override defaults. With
NetworkPolicy enabled, add native `extraIngress` rules for actual clients; `{}`
is a valid allow-all rule.
