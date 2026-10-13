# Helm operations

Start with [installation and common settings](../README.md).
[values.yaml](../values.yaml) documents every available parameter; this page
covers settings that need coordination or diagnosis.

## Credentials

Normal Helm installations generate/reuse three kept Secrets:

| Default Secret | Data key | Readers |
|---|---|---|
| `<release>-admin` | `password` | Server, Helm test |
| `<release>-auth-token` | `token_secret` | All Server replicas |
| `<release>-pd-auth` | `secret-key` | PD, Server storage wait, Hubble |

`existingSecret` takes priority over an inline value, which takes priority over
auto-generation. Existing Secrets are not managed by the chart. Override their
key names through the corresponding `key` fields.

The admin password must be printable ASCII without spaces, colons or
backslashes. The PD REST secret must be printable ASCII without surrounding
whitespace or backslashes. JWT signing keys must be at least 32 bytes.
These constraints match the current image's authentication/property encoding.

The admin Secret supplies the **initial** password when the account is created.
To rotate an existing account, first call the Server API with the old credential:

```bash
curl --user 'admin:<old-password>' \
  -X PUT -H 'Content-Type: application/json' \
  -d '{"user_password":"<new-password>"}' \
  http://<server-address>/graphspaces/DEFAULT/auth/users/admin
```

Update the Secret and its source values to the same new password, then restart
all Server replicas to clear cached credentials:

```bash
kubectl -n hugegraph rollout restart deployment/hugegraph-server
```

Use your secret manager rather than storing real passwords in public files.
GitOps needs fixed encrypted values or external Secrets, because offline
renderers cannot reuse a live generated Secret through `lookup`.
[Both complete examples](../examples/README.md#gitops-credentials) include all
three credential sources. Changing PD credentials rolls PD, Server and Hubble;
verify Gremlin and UI login after that coordinated rollout.

## Storage and upgrades

Inspect saved values before upgrading. `--reuse-values` preserves old effective
settings, including disabled Hubble, ClusterIP and explicit helper images.
Adopt new defaults only with reviewed values and `--reset-values`.

PD/Store PVC size and StorageClass live in StatefulSet claim templates and are
not routine mutable settings. For capacity growth, the StorageClass must support
expansion: resize each PVC, wait for completion, then coordinate an orphaned
StatefulSet recreation with matching claim-template values. Preserve Pod/claim
names and persisted data; do not recreate claims as a resize workaround.

PD/Store storage-mode changes are rejected after initialization. Hubble uses
an embedded H2 database with connection metadata and saved graph credentials;
a PVC and `Recreate` deployment strategy preserve it across Pod replacement.
Disabling Hubble or uninstalling does not delete kept claims/Secrets automatically.
Changing Hubble storage requires a separate reviewed metadata migration.

Local PVs bind to nodes and cannot move their files automatically on node loss.
See [all seven local PVs](../examples/README.md#storage) for directory, capacity,
claim binding and scheduling requirements. `Retain` preserves files on claim
deletion; reclamation/recovery needs explicit inspection.

## Startup and probes

Store's init container waits for a majority of PD peers on `store.waitPath`
(default `/v1/ready`) for up to `store.waitTimeoutSeconds` (default 900).
PD readiness requires a Raft leader; `/v1/health` only confirms a live listener.
Store's own readiness does not prove every shard has recovered.

Server waits for storage for `server.storageWaitTimeoutSeconds` (default 300),
passed as `WAIT_STORAGE_TIMEOUT_S`. An unspecified startup threshold is derived
from that wait plus a 150-second startup allowance. Explicit thresholds are
preserved and must leave at least 120 seconds beyond the storage wait.
The guaranteed probe budget is `(failureThreshold - 1) * periodSeconds`, since
the first probe can run immediately. Contradictory budgets fail rendering with
the relevant parameter names; `HG_SERVER_STARTUP_TIMEOUT_S` is chart-managed.
Legacy reused values retain their earlier probe-budget compatibility behavior.

`store.waitImage` and `server.waitImage` default empty and reuse the Store/Server
images, respectively. Those images already include `/bin/sh` and `curl`.
Explicit helper overrides remain supported for custom images. To clear an older
override without resetting other release settings:

```bash
helm upgrade hugegraph ./helm/hugegraph -n hugegraph --reuse-values \
  --set-string store.waitImage= --set-string server.waitImage=
```

## Topology and scheduling

The startup wrapper sets `usePD=true` and the chart PD peers so all Servers
share graph metadata. Distributed Servers keep `initStoreEnabled=false` and do
not concurrently run `init-store`. Each Server registers its Pod address with
PD unless `server.advertiseUrl` specifies one shared discovery address.

At first bootstrap, the chart seeds three shard replicas with at least three
Stores, or one otherwise. PD persists those settings. Chart changes after
initialization do not rewrite the partition metadata; use the PD configuration
API and coordinate partition patrol for existing groups.

PD Raft addresses, Store Raft addresses and StatefulSet names are persisted
identities. Do not change those hostnames/ports as an ordinary upgrade.
PD membership changes and Store shrink require explicit member/shard operations;
the chart rejects unsupported changes against initialized live StatefulSets.
Fresh installs do not perform the upgrade-only StatefulSet query.

Default preferred anti-affinity permits fewer nodes than replicas. The cluster
preset requires separate nodes for PD/Store; explicit affinity overrides presets.
Local PV node affinity must agree with scheduling rules. PDBs protect voluntary
eviction, not controller-driven updates. PD needs a majority available; Store
needs enough shard copies to remain available.

Server replicas can be changed routinely; CPU-utilization HPA requires positive
CPU requests. Store growth still needs volume and shard planning. Use chart
updates for Store scale changes so Hubble's endpoint list is updated too.

## Rolling Store updates and recovery

Use `store.updateStrategy.type=OnDelete` for a controlled rollout. Replace one
Store Pod at a time. Before replacing another, confirm the previous Pod is
Ready, its Store heartbeat is fresh, and every shard group reports full
membership and exactly one leader from the PD leader's view.
PD `Up` status alone does not prove shard recovery; stop on missing members or
leaders. The [website operations guide](https://hugegraph.apache.org/docs/quickstart/hugegraph/hugegraph-helm-operations/)
explains the rollout and recovery constraints.

Preserve volumes and recorded member identities during recovery. Deleting a
RocksDB lock file or assigning an empty volume does not recover shard data.
The chart does not provide backup or automatic disk-loss recovery; keep backups
outside the data PVs and validate recovery separately.

## Networking and Hubble

Easy mode leaves NetworkPolicy off; strict enables it. Explicit values win.
Policies require a network plugin that enforces them. Generated rules allow
component communication and DNS; admit external clients/controllers with native
`extraIngress`, and external addresses with appropriate `extraEgress`.
Empty external rule lists are valid and can block clients; `{}` is a valid
allow-all rule. Consider NodePort source NAT when choosing source ranges.

Client Services accept native Kubernetes traffic and load-balancer fields under
`pd.service`, `server.service` and `hubble.service`. LoadBalancer-specific fields
need that Service type and a compatible controller. Hostless HTTP Ingress is
supported; install an Ingress controller separately. TLS is optional.

In-chart Hubble is wired to internal PD/Server endpoints. `hubble.mode=direct`
uses only the Server Service and has no PD operations view. External Hubble
needs reachable Server addresses; PD discovery additionally needs reachable PD
gRPC and a suitable `server.advertiseUrl`. That override affects every discovery
client, including in-cluster Hubble. Current Hubble login requires Server auth;
use `hubble.enabled=false` for an unauthenticated API-only deployment.

## Troubleshooting

| Symptom | First checks |
|---|---|
| PVC Pending | StorageClass/provisioner, matching PVs, PVC and Pod events; initial `WaitForFirstConsumer` Pending can be normal |
| Mount failure | Host directory, node affinity and image UID/GID permissions |
| ImagePullBackOff | Pod events, registry reachability, image tags/digests and `imagePullSecrets` |
| `HG_PD_AUTH_SECRET_KEY` missing | PD Pod Secret reference and key; chart injects the shared PD REST password automatically |
| Server storage wait timeout | Ready PD majority, Store registration, network policies and `server.storageWaitTimeoutSeconds` |
| NodePort/Ingress unreachable | Assigned port, reachable node/controller address, firewall and NetworkPolicy rules |
| REST works but Gremlin reports `Could not rebind` | Run `helm test`; after PD stabilizes, replace the failing Server Pod and retry |

```bash
kubectl -n hugegraph get pods,pvc,svc
kubectl -n hugegraph describe pod <pod-name>
kubectl -n hugegraph logs <pod-name>
helm test hugegraph -n hugegraph --logs --timeout 5m
```

Never print credential values in diagnostic logs. Ready and `/versions` do not
prove graph binding or fresh UI login; verify graph queries and a new login too.
