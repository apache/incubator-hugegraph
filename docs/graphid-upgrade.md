# HStore GraphID checks before upgrading from 1.7

Some 1.7 batch PUT/MERGE paths wrote business keys with the reserved GraphID
`0xFFFE` when no mapping existed. Current Store allocates a real GraphID for
these operations. The allocation can also happen during Raft replay, before a
client sends a request. Existing `0xFFFE` rows then become invisible to reads
through the new mapping. Replacing the Store JAR with 1.7 does not undo a mapping
that has already been persisted.

This is an HStore upgrade issue. A 1.7 database using only valid allocated
GraphIDs does not need its keys converted for this issue.

## Check each Store before the first 1.8 startup

Keep a complete, matching backup of the 1.7 PD, every Store (data **and** Raft
paths), Server configuration, and the original input where available. Quiesce
all writers, take snapshots with 1.7, and stop the Stores before copying their
files. A stopped process alone does not prove that WAL-disabled writes were
flushed; retain the Raft logs together with the data and snapshots.

From the 1.8 Store distribution, using that Store's effective local configuration:

```bash
bin/hugegraph-store-check.sh --check
```

An explicit configuration directory and multiple data roots are supported:

```bash
bin/hugegraph-store-check.sh --check \
  --config /srv/hugegraph-store/conf \
  --data-path /srv/store-data-a,/srv/store-data-b
```

Run the command with Java 17, selected through `JAVA_HOME` or `java` on `PATH`.
It uses the RocksDB dependencies already in the Store distribution, needs no
running PD, Store or Server, and creates no database. It reads `application.yml`
followed by `application-pd.yml` when present. It does not start Spring or resolve
its environment, JSON or profile overrides. If the deployment uses those
overrides, pass the actual data roots with `--data-path` and point `--config` at
local configuration files containing the resolved effective RocksDB settings.
For example, a Docker deployment's `HG_STORE_DATA_PATH` must be supplied as
`--data-path`; the command does not read the entrypoint's `SPRING_APPLICATION_JSON`.
Relative paths are resolved from the Store distribution, as with its startup scripts.
Do not point the command at a running Store: it refuses locked active databases,
and a separate read-only database cannot observe another process's memtables.

| Result | Meaning | Next step |
| --- | --- | --- |
| Exit 0 | No reserved-prefix rows or invalid GraphID mappings were found in the persisted databases and latest snapshots inspected | Keep the backups and follow the normal release upgrade procedure; this check does not certify the entire upgrade |
| Exit 2 with `Legacy GraphID 0xFFFE` | The reported database/column family contains incompatible historical rows | Use the recovery path below before starting 1.8 on this data |
| Exit 2 with an invalid mapping, lock, configuration or I/O error | Inspection could not establish a clean persisted state | Resolve the reported problem and run the check again; do not treat this as a clean result |

A launcher or JVM failure can return another nonzero status. Treat every nonzero
status as an unsuccessful check.

Run the check on **every Store**. Users do not need to supply numeric GraphIDs or
partition IDs. The check visits configured data roots, retained DB versions and
the latest local snapshot for each partition. It also considers stored locations
that are not remapped from the configured roots. It seeks the reserved prefix
once per column family and reads GraphID metadata; it does not scan every
business row. Retained versions are inspected without invoking the normal
session opener, which can delete older versions.

Store startup performs the same read-only inspection before the Store engine,
RocksDB factory, Raft groups and Store registration initialize. On failure the
process exits unsuccessfully instead of leaving a partially initialized Store
advertising healthy partitions. The earlier configuration listener can still
contact PD or synchronize local configuration. The guarantee here concerns the
Store databases and registration, not all configuration activity.

Incoming snapshots are checked before opening or replacing the local partition
database. A rejected snapshot does not advance the state machine's committed
index. Local snapshots are also checked at startup: a `should_not_load` marker
only skips local installation and does not stop a checkpoint reaching another
replica.

## Choose the recovery source first

Do not infer that every `0xFFFE` row belongs to the graph named in the Server
configuration. Different graphs, including the automatically created
`DEFAULT/~sys_graph/g`, could share the same physical namespace. System variables
can occupy vertex and index tables too. Reassigning the entire prefix to one
graph can silently assign data to the wrong owner.

| Historical state | Supported action |
| --- | --- |
| Valid GraphID mappings and no incompatible rows | No GraphID-specific data conversion |
| A trusted 1.7 source still returns all expected application data | Validate a logical export and restore it into a **new, empty** 1.8 cluster |
| New and old prefixes coexist, including after replay, writes or truncate | Preserve the affected cluster. Recover a matching trusted pre-upgrade source or original input first; do not union the prefixes automatically |
| Several graphs shared the prefix | Validate each graph's ownership and complete application data against independent input/backups before exporting. Refuse an automatic reassignment when ownership is uncertain |
| A physical key was overwritten or deleted | No mapping or prefix conversion can recover the lost value without another copy |

An empty new prefix, or equal values at currently colliding keys, does not prove
that combining prefixes is safe: an earlier delete or truncate can make a merge
resurrect data the application already removed. A disk inspection cannot prove
historical ownership or the absence of those operations.

If 1.8 has already opened the data, do not use JAR rollback as a recovery
procedure. Bring up a **copy** of the matching 1.7 backup with its original
configuration in an isolated recovery environment. Verify the source through
fresh requests, not a Server cache retained across the upgrade. Keep the
original backup immutable.

## Logical migration with existing HugeGraph Tools

For a validated, readable application graph, the existing Tools `backup` and
`restore` commands avoid editing RocksDB keys, allocator metadata, Raft logs or
snapshots. Restore uses the normal Server/Store API and Raft replication. The
target cluster creates its own mappings, replicas and snapshots. Use independent
PD and Store data paths and endpoints for the empty target; never attach an empty
target to the source PD or reuse the source Store identity.

The example below is for a graph in the **DEFAULT GraphSpace**, without special
state outside the export scope described below. Use a compatible HugeGraph Tools
release; the tested source and version are listed in the validation section.
Keep application writers stopped until export and validation finish. Keep target
application traffic disabled until all checks pass.

Use new, empty output and log directories for **each attempt**. Reusing a backup
directory can overwrite an earlier export and retain stale shard files from it.
`restore --clean` deletes the local backup; it is deliberately absent below.

```bash
# Run from the HugeGraph Tools distribution. Set these for the isolated clusters.
SOURCE_URL=http://source-server:8080
TARGET_URL=http://target-server:8080
GRAPH=hugegraph
BACKUP=/srv/upgrade/backup-attempt-1
BACKUP_LOG=/srv/upgrade/backup-log-attempt-1
RESTORE_LOG=/srv/upgrade/restore-log-attempt-1
VERIFY_BACKUP=/srv/upgrade/verify-attempt-1
VERIFY_LOG=/srv/upgrade/verify-log-attempt-1

bin/hugegraph --url "$SOURCE_URL" --graph "$GRAPH" backup \
  --directory "$BACKUP" --log "$BACKUP_LOG" --huge-types all \
  --format json --compress false --thread-num 1

# Verify the source export and failure logs before continuing (see below).
bin/hugegraph --url "$TARGET_URL" --graph "$GRAPH" graph-mode-set \
  --graph-mode RESTORING

bin/hugegraph --url "$TARGET_URL" --graph "$GRAPH" restore \
  --directory "$BACKUP" --log "$RESTORE_LOG" --huge-types all --thread-num 1

bin/hugegraph --url "$TARGET_URL" --graph "$GRAPH" backup \
  --directory "$VERIFY_BACKUP" --log "$VERIFY_LOG" --huge-types all \
  --format json --compress false --thread-num 1

# Only after complete comparison, query checks and restart verification:
bin/hugegraph --url "$TARGET_URL" --graph "$GRAPH" graph-mode-set \
  --graph-mode NONE
```

Adjust the worker count for the deployment after establishing correctness.
The example uses a single worker to make a small validation run
straightforward; it is not a throughput recommendation.

### Validate completeness before cutover

A zero exit status is necessary but insufficient. The tested Tools version can
log a failed backup shard and still exit successfully. Check its separate
vertices/edges `failed_shards`, `timeout_shards` and `limit_exceed_shards` files;
any nonempty such file means the export is incomplete. Preserve the logs and
start a new attempt in new directories after resolving the error.

Compare the source export with independent expected data, then compare source
and target exports. Include all four schema types and their IDs; vertex IDs,
labels and properties; edge IDs, labels, endpoints, direction and properties;
record counts and ID uniqueness. Ignore transport-only schema controls and
normalize unordered sets, but preserve ordered list properties. `RESTORING`
retains schema IDs; the tool can recompute primary-key vertex IDs from the label
ID and key properties, so verify IDs explicitly.

Run application queries that exercise indexes, adjacency, filters and graph
isolation. Repeat the read checks after a cold target restart and verify a new
write. On replicated deployments, verify failover and snapshot installation on a
recovering replica before accepting the target. Counts alone are not an adequate
migration oracle.

If restore fails or is interrupted, leave target application traffic disabled.
For this upgrade, use another empty target or recreate only the disposable target
from its initial empty state and rerun the complete restore from the retained,
verified export. Do not merge an uncertain partial restore into the source. A
failed command can leave the target in `RESTORING`; inspect its mode explicitly.
Keep source backups through the acceptance period. Once applications write to the
target, returning to the source requires reconciling those writes; it is no
longer a file or JAR rollback.

### Export scope and release limits

Tools exports property keys, vertex labels, edge labels, index labels, vertices
and edges. This is not a full Store/PD/system-state backup. Authentication data
has separate Tools commands. Graph variables, system KV, task state, OLAP state
and operational configuration need a separate inventory and recovery procedure.
Do not restore the shared historical system namespace by assigning it to the
user graph.

The inspected Tools CLI addresses DEFAULT GraphSpace and has no `--graphspace`
option. Other GraphSpaces require an explicitly GraphSpace-aware API/client
export and a separately verified restore. Do not substitute a graphspace-prefixed
graph name in these commands. TTL expiration timestamps are another boundary:
restoring a TTL label without `ttl_start_time` can restart its lifetime. A plain
logical export must not be presented as preserving those original timestamps.

Deployments needing these features must validate their additional recovery path
before upgrading. Shared-prefix ownership that cannot be established and lost
rows without backups remain recovery blockers. The preflight intentionally has
no override that silently assigns ambiguous data.

## Validation basis

The issue and original allocation fix are documented in
[apache/hugegraph#3224](https://github.com/apache/hugegraph/issues/3224),
[#3095](https://github.com/apache/hugegraph/issues/3095) and
[#3153](https://github.com/apache/hugegraph/pull/3153). The related preflight and
upgrade documentation proposals are
[#3330](https://github.com/apache/hugegraph/pull/3330) and
[hugegraph-doc#523](https://github.com/apache/hugegraph-doc/pull/523).

The independent test baseline uses the official 1.7.0 distribution and
[the issue author's validation data](https://github.com/SebastianGruza/hugegraph-validation).
The reference current Store commit is `c2ec5704332fc1102ad277d6aae6658d65c73601`;
Tools is `apache/hugegraph-toolchain@411aca3389dd610d9a4f3b3595f06a53ee67e2ff`.
The author's script creates 29 vertices and 63 edges. The experiment adds five
custom-ID vertices after a snapshot, giving the 34-vertex/63-edge baseline, and
reuses its 16 queries. Comparison includes full IDs, properties and edge endpoints. Two partitions were exercised with both one Store and three Store
replicas. See the change's test report for the exact completed upgrade, restart
and fault scenarios; native unit tests are distinct from those cluster runs.

This fix adds no second read/write format, GraphID allocator rule, Raft command
or recurring scan to normal requests. It does not establish general rolling
upgrade/rollback compatibility for the release; see
[apache/hugegraph#3223](https://github.com/apache/hugegraph/issues/3223).
