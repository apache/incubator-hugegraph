# HugeGraph HStore Helm Chart

Deploys **3 PD + 3 Store + 3 Server and Hubble** by default, with authentication
and persistent storage enabled. Requires **Helm 3.10+ and Kubernetes 1.23+**.
The OCI chart is named `hugegraph-helm`; the application and default Kubernetes resource names remain
`hugegraph`. This chart targets HugeGraph 1.8.0 and later; earlier service releases
are not supported.

Provide a default StorageClass or prepare matching PVs. The default installation
needs **7 volumes**: 3 × 10 GiB for PD, 3 × 50 GiB for Store, and 1 × 1 GiB
for Hubble. The chart creates PVCs; it does not create a storage provisioner.

## Install a published Chart

For a quick trial, install the latest available Chart from GHCR:

```bash
kubectl config current-context
kubectl get storageclass
helm install hugegraph oci://ghcr.io/hugegraph/hugegraph-helm --devel \
  -n hugegraph --create-namespace --wait --timeout 15m
```

> [!TIP]
> `--devel` selects the highest Chart version, including development and stable
> versions, without limiting the version series. For production or reproducible
> installs, replace it with `--version <published-version>` and review the images.

See [distribution and offline installation](docs/distribution.md) for Docker Hub,
the single-node preset and internal OCI/HTTP repositories. Development Charts may
use `latest` images; pin component `image.digest` values when needed.
The source installation and configuration examples below assume a repository
checkout and commands run from its root.

## Install from source and open Hubble

```bash
helm install hugegraph ./helm/hugegraph -n hugegraph \
  --create-namespace --wait --timeout 15m

# Find assigned ports and a reachable node IP
kubectl -n hugegraph get svc hugegraph-server hugegraph-hubble
kubectl get nodes -o wide

# Read the initial admin password
kubectl -n hugegraph get secret hugegraph-admin \
  -o jsonpath='{.data.password}' | base64 --decode
```

Open **`http://<node-ip>:<hubble-node-port>`** and log in as `admin` with that
password. Server API uses `http://<node-ip>:<server-node-port>`.
HTTP works directly; a hostname, certificate or extra acknowledgement is not
required. PD remains internal.

If node access is unavailable, use port-forward:

```bash
kubectl -n hugegraph port-forward svc/hugegraph-hubble 8088:8088
# Open http://127.0.0.1:8088
```

Commands assume release/namespace `hugegraph`. For another release, use the names
shown by `kubectl get svc,secret -n <namespace>`. Default workloads use
`<fullname>-<component>`; generated Secrets use `<release>-admin`,
`<release>-auth-token`, and `<release>-pd-auth`.

## Common configurations

### API only

Hubble is optional. Current Hubble login requires Server authentication.

```bash
# Authenticated API, without the UI
helm install hugegraph ./helm/hugegraph -n hugegraph --create-namespace \
  --set hubble.enabled=false

# Also disable API authentication, if wanted
# Add: --set server.auth.enabled=false
```

Inactive credential values may remain in the values file when auth is disabled.

### Internal image registry

Mirror the four component images (`hugegraph/pd`, `hugegraph/store`,
`hugegraph/server`, `hugegraph/hubble`) at the configured tags or digests:

```bash
helm install hugegraph ./helm/hugegraph -n hugegraph --create-namespace \
  --set global.imageRegistry=registry.internal.example.com
```

A component repository with an explicit registry takes priority over the global
prefix. Set `imagePullSecrets` if the registry needs credentials.
Store's wait init container and Helm test reuse the Store and Server images,
which already contain `curl`; no separate curl image is needed by default.
See the [complete registry example](examples/values-internal-registry.yaml).

### Storage

Use a named dynamic StorageClass when the cluster has no suitable default:

```bash
helm install hugegraph ./helm/hugegraph -n hugegraph --create-namespace \
  --set pd.storage.storageClassName=my-csi \
  --set store.storage.storageClassName=my-csi \
  --set hubble.persistence.storageClassName=my-csi
```

Empty `storageClassName` inherits the cluster default. `"-"` explicitly selects
no StorageClass. Neither creates a provisioner. For local disks, prepare host
directories and PVs first; the [seven-volume local example](examples/README.md#storage)
includes node affinity, capacities and scheduling requirements.

For a **new temporary installation**, add `--set persistence.enabled=false`
to use `emptyDir`. Component storage flags override that global setting.
PVC failures never cause an automatic fallback. Do not switch initialized
PD/Store data between persistent and temporary storage.

### Server service identity

`server.serviceId` and `server.graphSpace` select the Server's PD service metadata.
Both default to `DEFAULT`, which preserves the image configuration. Non-default
values inject `HG_SERVER_SERVICE_ID` and `HG_SERVER_GRAPHSPACE` for the image
entrypoint. Service IDs accept printable ASCII, including spaces and backslashes;
the full name, including `/` and `-`, survives metadata reload and graph-space
clearing. Dollar signs remain literal through Kubernetes environment expansion.
Graph spaces must be `DEFAULT` or up to 48 lowercase letters, digits and
underscores, starting with a letter. Create the target space before starting its
assigned Server; these settings do not create it.

```bash
helm upgrade --install hugegraph ./helm/hugegraph -n hugegraph \
  --set-string server.serviceId=analytics \
  --set-string server.graphSpace=team_a \
  --set-string server.testGraphAlias=team_a-demo
```

Use a chart and Server image build from the same source version containing these
mappings. Older release images without them ignore these environment settings;
select a matching image build explicitly. The chart does not detect old images.
The Server's existing `SERVICE_ID` normalization (`GraphManager.serviceId()`) is
unchanged: select `GRAPHSPACE` and the complete raw `SERVICE_NAME` in discovery
queries. Identifier-only lookup has no uniqueness or isolation guarantee.

`server.testGraphAlias` selects an existing Gremlin binding for every Server Pod,
defaulting to `DEFAULT-hugegraph`. It does not create a graph or infer its binding
from `server.graphSpace`. Match the binding's case: `Demo.properties` can register
`DEFAULT-Demo`, while REST-created graph names are lowercase. A successful query
of the default local binding does not verify a graph in `team_a`. Before replacing
a Pod after `Could not rebind`, verify that the graph exists and the selected alias
matches its binding; restarting cannot create a missing graph or correct an alias.

These settings do not change the REST default path graph space or Hubble's
selected service. Use explicit `/graphspaces/team_a/...` paths and the matching
Hubble selection. Non-default discovery, authentication and graph-space isolation
still need verification with a running Linux cluster. See the
[container identity contract](../../docker/README.md#server-service-identity).

## Modes and entry points

`mode: easy` is the default. `mode: strict` changes Server/Hubble to ClusterIP
and enables NetworkPolicy; authentication, Hubble and persistence stay enabled.
Use `--set mode=strict` or `-f helm/hugegraph/values-strict.yaml`.

**Explicit component value > global value > mode default.** An explicit `false`
is preserved; null component values inherit. The global settings are
`persistence.enabled` and `global.imageRegistry`. No mode requires HTTPS.

| Entry | Access | When to use it |
|---|---|---|
| NodePort | Reachable node IP + assigned port | Direct access without another controller; easy default |
| ClusterIP | Internal Service IP/DNS | In-cluster clients, an existing gateway or port-forward |
| LoadBalancer | Infrastructure-assigned address | A load balancer controller is available |
| Ingress | HTTP routes through a controller | Shared HTTP routing; hostname and TLS are optional |

ClusterIP is an internal virtual service address, not a node IP. An ordinary
laptop usually cannot reach it directly. With default `externalTrafficPolicy:
Cluster`, NodePort can forward through a node that does not host the target Pod.

To fix a NodePort, set `server.service.nodePort` or `hubble.service.nodePort`.
Omission or `0` allocates automatically. Kubernetes validates the actual cluster
range; the chart does not force `30000–32767`.

A [hostless HTTP Ingress example](examples/values-http-ingress.yaml) is available.
With NetworkPolicy enabled, add native rules for actual clients and controllers;
empty external rule lists are valid but can block access. Explicit Service and
policy values override either mode. Legacy exposure acknowledgement fields are
accepted for compatibility and no longer gate rendering.

## Common parameters

Defaults below describe a fresh easy-mode install. Supply `--set` or a YAML file
with `-f`; see [values.yaml](values.yaml) for all fields and their comments.

| Parameter | Meaning | Default |
|---|---|---|
| `mode` | Entry and policy defaults | `easy` |
| `pd.replicas` / `store.replicas` / `server.replicas` | Component replicas | `3` / `3` / `3` |
| `hubble.enabled` | Install the UI | `true` |
| `server.auth.enabled` | Server authentication | `true` |
| `server.service.type` / `hubble.service.type` | Explicit entry type; null inherits mode | resolved `NodePort` |
| `pd.service.type` | PD client entry type | `ClusterIP` |
| `*.service.nodePort` | Fixed Server/Hubble port; omitted/0 allocates automatically | unset |
| `global.imageRegistry` | Prefix image repositories without an explicit registry | `""` |
| `*.image.repository` / `tag` / `digest` | Component image; digest takes priority | see values |
| `persistence.enabled` | Global persistent storage | `true` |
| `pd.storage.enabled` / `store.storage.enabled` / `hubble.persistence.enabled` | Explicit component storage override | `null` (inherits) |
| `*.storageClassName` | Cluster default when empty; `"-"` means no class | `""` |
| `pd.storage.size` / `store.storage.size` / `hubble.persistence.size` | Volume capacities | `10Gi` / `50Gi` / `1Gi` |
| `server.serviceId` / `server.graphSpace` | PD service name and existing graph space | `DEFAULT` / `DEFAULT` |
| `server.testGraphAlias` | Existing Gremlin binding checked by `helm test` | `DEFAULT-hugegraph` |
| `server.storageWaitTimeoutSeconds` | Storage-ready deadline | `300` |
| `networkPolicy.enabled` | Explicit policy override; null inherits mode | resolved `false` |

<details>
<summary>Advanced settings and presets</summary>

- [Operations](docs/operations.md): credentials, probes, upgrades, membership,
  scheduling, policies and troubleshooting.
- [Deployment examples](examples/README.md): private registry, local volumes,
  fixed GitOps credentials, external Secrets, HTTP Ingress and API-only usage.
- `values-single.yaml`: 1 PD + 1 Store + 1 Server, with Hubble and authentication.
- `values-cluster.yaml`: required PD/Store anti-affinity, resources and controlled
  Store updates. NetworkPolicy inherits mode; the preset is a capacity starting point.
- All client Services support native `externalTrafficPolicy`, `loadBalancerClass`,
  `loadBalancerIP`, `loadBalancerSourceRanges`, `externalIPs` and
  `allocateLoadBalancerNodePorts`, subject to cluster/controller support.
- Resources, JVM options, scheduling, probes, HPA/PDB, metadata and external
  Hubble discovery are configured under their components in `values.yaml`.
  Default images preserve automatic JVM sizing; set resources for your capacity.

</details>

## Upgrade and verify

```bash
helm get values hugegraph -n hugegraph --all
helm upgrade hugegraph ./helm/hugegraph -n hugegraph --reuse-values \
  --wait --timeout 15m
helm test hugegraph -n hugegraph --logs --timeout 5m
```

`--reuse-values` preserves existing UI, entry, storage and credential settings.
An easy-mode upgrade that inherits Server/Hubble entry types prints a short
NodePort-default notice in NOTES; explicitly configured entry types are preserved.
To adopt new defaults, use `--reset-values` with a reviewed values file retaining
all settings your release needs. Older explicit helper images also remain under
value reuse; clear `store.waitImage` and `server.waitImage` to reuse component images.

The admin Secret is an **initial credential**. Changing it alone does not rotate
an existing account; follow [credential rotation](docs/operations.md#credentials).
The chart checks persisted Raft identities and storage/membership changes during
upgrades; those changes require a coordinated procedure.

`helm --wait` checks readiness. `helm test` checks graph binding and visible
associated indexes for `CREATED` status on each Server Pod. Run it after index
creation/rebuild jobs finish, with no schema changes during the test. On HStore,
reading an unfinished index can cache its temporary status on other Pods;
retries do not refresh that state, and indexed queries may keep failing until
those Pods restart after the jobs finish. It does not prove index query results; add
[business assertions](docs/operations.md#business-query-assertions)
with `--set-file server.testGremlin=assertions.groovy`. Also verify a fresh Hubble
login and graph query. Templates and linting alone do not prove live volume,
network or application behavior.

Website guides: [Helm deployment](https://hugegraph.apache.org/docs/quickstart/hugegraph/hugegraph-helm/)
and [Kubernetes operations](https://hugegraph.apache.org/docs/quickstart/hugegraph/hugegraph-helm-operations/).
