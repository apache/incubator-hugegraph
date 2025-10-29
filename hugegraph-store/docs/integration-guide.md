# Integration Guide

This guide explains how to integrate HugeGraph Store with HugeGraph Server, use the client library, and migrate from other storage backends.

## Table of Contents

- [Backend Configuration](#backend-configuration)
- [Client Library Usage](#client-library-usage)
- [Integration with PD](#integration-with-pd)
- [Migration from Other Backends](#migration-from-other-backends)
- [Multi-Graph Configuration](#multi-graph-configuration)
- [Troubleshooting Integration Issues](#troubleshooting-integration-issues)

---

## Backend Configuration

### Configuring HugeGraph Server to Use Store

HugeGraph Store is configured as a pluggable backend in HugeGraph Server.

#### Step 1: Edit Graph Configuration

File: `hugegraph-server/conf/graphs/<graph-name>.properties`

**Basic Configuration**:
```properties
# Backend type
backend=hstore
serializer=binary

# Store provider class
store.provider=org.apache.hugegraph.backend.store.hstore.HstoreProvider

# PD cluster endpoints (required)
store.pd_peers=192.168.1.10:8686,192.168.1.11:8686,192.168.1.12:8686

# Connection pool
store.max_sessions=4
store.session_timeout=30000

# Graph name
graph.name=hugegraph
```

**Advanced Configuration**:
```properties
# gRPC settings
store.grpc_max_inbound_message_size=104857600  # 100MB

# Retry settings
store.max_retries=3
store.retry_interval=1000  # milliseconds

# Batch settings
store.batch_size=500

# Timeout settings
store.rpc_timeout=30000  # RPC timeout in milliseconds
```

#### Step 2: Initialize Schema

```bash
cd hugegraph-server

# Initialize backend storage (creates system schema)
bin/init-store.sh

# Expected output:
# Initializing HugeGraph Store backend...
# Connecting to PD: 192.168.1.10:8686,192.168.1.11:8686,192.168.1.12:8686
# Creating system tables...
# Initialization completed successfully
```

**What happens during initialization**:
1. Server connects to PD cluster
2. PD provides Store node addresses
3. Server creates system schema (internal metadata tables)
4. Server creates graph-specific schema tables

#### Step 3: Start HugeGraph Server

```bash
# Start server
bin/start-hugegraph.sh

# Check logs
tail -f logs/hugegraph-server.log

# Look for successful backend initialization:
# INFO  o.a.h.b.s.h.HstoreProvider - HStore backend initialized successfully
# INFO  o.a.h.b.s.h.HstoreProvider - Connected to PD: 192.168.1.10:8686
# INFO  o.a.h.b.s.h.HstoreProvider - Discovered 3 Store nodes
```

#### Step 4: Verify Backend

```bash
# Check backend via REST API
curl http://localhost:8080/graphs/hugegraph/backend

# Expected response:
{
  "backend": "hstore",
  "version": "1.7.0",
  "nodes": [
    {"id": "1", "address": "192.168.1.20:8500"},
    {"id": "2", "address": "192.168.1.21:8500"},
    {"id": "3", "address": "192.168.1.22:8500"}
  ],
  "partitions": 12
}
```

---

## Client Library Usage

The `hg-store-client` module provides a Java client for directly interacting with Store clusters (typically used by HugeGraph Server, but can be used standalone).

### Maven Dependency

```xml
<dependency>
    <groupId>org.apache.hugegraph</groupId>
    <artifactId>hg-store-client</artifactId>
    <version>1.7.0</version>
</dependency>
```

### Basic Usage

#### 1. Creating a Client

```java
import org.apache.hugegraph.store.client.HgStoreClient;
import org.apache.hugegraph.store.client.HgStoreSession;

// PD addresses
String pdPeers = "192.168.1.10:8686,192.168.1.11:8686,192.168.1.12:8686";

// Create client
HgStoreClient client = HgStoreClient.create(pdPeers);

// Create session for a graph
String graphName = "hugegraph";
HgStoreSession session = client.openSession(graphName);
```

#### 2. Basic Operations

**Put (Write)**:
```java
import org.apache.hugegraph.store.client.HgStoreSession;

// Put a key-value pair
byte[] key = "vertex:person:1001".getBytes();
byte[] value = serializeVertex(vertex);  // Your serialization logic

session.put(tableName, key, value);
```

**Get (Read)**:
```java
// Get value by key
byte[] key = "vertex:person:1001".getBytes();
byte[] value = session.get(tableName, key);

if (value != null) {
    Vertex vertex = deserializeVertex(value);
}
```

**Delete**:
```java
// Delete a key
byte[] key = "vertex:person:1001".getBytes();
session.delete(tableName, key);
```

**Scan (Range Query)**:
```java
import org.apache.hugegraph.store.client.HgStoreResultSet;

// Scan all keys with prefix "vertex:person:"
byte[] startKey = "vertex:person:".getBytes();
byte[] endKey = "vertex:person:~".getBytes();

HgStoreResultSet resultSet = session.scan(tableName, startKey, endKey);

while (resultSet.hasNext()) {
    HgStoreResultSet.Entry entry = resultSet.next();
    byte[] key = entry.key();
    byte[] value = entry.value();

    // Process entry
}

resultSet.close();
```

#### 3. Batch Operations

```java
import org.apache.hugegraph.store.client.HgStoreBatch;

// Create batch
HgStoreBatch batch = session.beginBatch();

// Add operations to batch
for (Vertex vertex : vertices) {
    byte[] key = vertexKey(vertex.id());
    byte[] value = serializeVertex(vertex);
    batch.put(tableName, key, value);
}

// Commit batch (atomic write via Raft)
batch.commit();

// Or rollback
// batch.rollback();
```

#### 4. Session Management

```java
// Close session
session.close();

// Close client (releases all resources)
client.close();
```

### Advanced Usage

#### Query with Filters

```java
import org.apache.hugegraph.store.client.HgStoreQuery;
import org.apache.hugegraph.store.client.HgStoreQuery.Filter;

// Build query with filter
HgStoreQuery query = HgStoreQuery.builder()
    .table(tableName)
    .prefix("vertex:person:")
    .filter(Filter.eq("age", 30))  // Filter: age == 30
    .limit(100)
    .build();

// Execute query
HgStoreResultSet resultSet = session.query(query);

while (resultSet.hasNext()) {
    // Process results
}
```

#### Aggregation Queries

```java
import org.apache.hugegraph.store.client.HgStoreQuery.Aggregation;

// Count vertices with label "person"
HgStoreQuery query = HgStoreQuery.builder()
    .table(tableName)
    .prefix("vertex:person:")
    .aggregation(Aggregation.COUNT)
    .build();

long count = session.aggregate(query);
System.out.println("Person count: " + count);
```

#### Multi-Partition Iteration

```java
// Scan across all partitions (Store handles partition routing)
HgStoreResultSet resultSet = session.scanAll(tableName);

while (resultSet.hasNext()) {
    HgStoreResultSet.Entry entry = resultSet.next();
    // Process entry from any partition
}

resultSet.close();
```

### Connection Pool Configuration

```java
import org.apache.hugegraph.store.client.HgStoreClientConfig;

// Configure client
HgStoreClientConfig config = HgStoreClientConfig.builder()
    .pdPeers(pdPeers)
    .maxSessions(10)               // Max sessions per Store node
    .sessionTimeout(30000)          // Session timeout (ms)
    .rpcTimeout(10000)              // RPC timeout (ms)
    .maxRetries(3)                  // Max retry attempts
    .retryInterval(1000)            // Retry interval (ms)
    .build();

HgStoreClient client = HgStoreClient.create(config);
```

---

## Integration with PD

### Service Discovery Flow

```
1. Server/Client starts with PD addresses
   ↓
2. Connect to PD cluster (try each peer until success)
   ↓
3. Query PD for Store node list
   ↓
4. PD returns Store nodes and their addresses
   ↓
5. Client establishes gRPC connections to Store nodes
   ↓
6. Client queries PD for partition metadata
   ↓
7. Client caches partition → Store mapping
   ↓
8. For each operation:
   - Hash key to determine partition
   - Look up partition's leader Store
   - Send request to leader Store
```

### Partition Routing

**Example**: Write vertex with ID `"person:1001"`

```java
// 1. Client hashes the key
String key = "vertex:person:1001";
int hash = MurmurHash3.hash32(key);  // e.g., 0x12345678

// 2. Client queries PD: which partition owns this hash?
Partition partition = pdClient.getPartitionByHash(graphName, hash);
// PD responds: Partition 5

// 3. Client queries PD: who is the leader of Partition 5?
Shard leader = partition.getLeader();
// PD responds: Store 2 (192.168.1.21:8500)

// 4. Client sends write request to Store 2
storeClient.put(leader.getStoreAddress(), tableName, key, value);
```

**Caching**:
- Client caches partition metadata (refreshed every 60 seconds)
- On leader change, client receives redirect response and updates cache

### Handling PD Failures

**Scenario**: PD cluster is temporarily unavailable

**Client Behavior**:
1. **Short outage** (<60 seconds):
   - Client uses cached partition metadata
   - Operations continue normally
   - Client retries PD connection in background

2. **Long outage** (>60 seconds):
   - Cached metadata may become stale (e.g., leader changed)
   - Client may send requests to wrong Store node
   - Store node redirects client to current leader
   - Client updates cache and retries

3. **Complete PD failure**:
   - Client cannot discover new Store nodes or partitions
   - Existing operations work, but cluster cannot scale or rebalance

**Recommendation**: Always run PD in a 3-node or 5-node cluster for high availability

---

## Migration from Other Backends

### RocksDB Embedded to Store

**Use Case**: Migrating from single-node RocksDB backend to distributed Store

#### Step 1: Backup Existing Data

```bash
# Using HugeGraph-Tools (Backup & Restore)
cd hugegraph-tools

# Backup graph data
bin/hugegraph-backup.sh \
  --graph hugegraph \
  --directory /backup/hugegraph-20250129 \
  --format json

# Backup completes, creates:
# /backup/hugegraph-20250129/
#   ├── schema.json
#   ├── vertices.json
#   └── edges.json
```

#### Step 2: Deploy Store Cluster

Follow [Deployment Guide](deployment-guide.md) to deploy PD and Store clusters.

#### Step 3: Configure Server for Store Backend

Edit `conf/graphs/hugegraph.properties`:

```properties
# Change from:
# backend=rocksdb

# To:
backend=hstore
store.provider=org.apache.hugegraph.backend.store.hstore.HstoreProvider
store.pd_peers=192.168.1.10:8686,192.168.1.11:8686,192.168.1.12:8686
```

#### Step 4: Initialize Store Backend

```bash
# Initialize Store backend (creates schema)
bin/init-store.sh
```

#### Step 5: Restore Data

```bash
# Restore data to Store backend
cd hugegraph-tools

bin/hugegraph-restore.sh \
  --graph hugegraph \
  --directory /backup/hugegraph-20250129 \
  --format json

# Restore progress:
# Restoring schema... (100%)
# Restoring vertices... (1,000,000 vertices)
# Restoring edges... (5,000,000 edges)
# Restore completed successfully
```

#### Step 6: Verify Migration

```bash
# Check vertex count
curl http://localhost:8080/graphs/hugegraph/graph/vertices?limit=0

# Check edge count
curl http://localhost:8080/graphs/hugegraph/graph/edges?limit=0

# Run sample queries
curl http://localhost:8080/graphs/hugegraph/graph/vertices?label=person&limit=10
```

---

### MySQL/PostgreSQL to Store

**Use Case**: Migrating from relational database backends

#### Option 1: Using Backup & Restore (Recommended)

Same steps as RocksDB migration above.

#### Option 2: Using HugeGraph-Loader (For ETL)

If you need to transform data during migration:

```bash
# 1. Export data from MySQL backend
# (Use mysqldump or HugeGraph API)

# 2. Create loader config
cat > load_config.json <<EOF
{
  "vertices": [
    {
      "label": "person",
      "input": {
        "type": "file",
        "path": "vertices_person.csv"
      },
      "mapping": {
        "id": "id",
        "name": "name",
        "age": "age"
      }
    }
  ],
  "edges": [
    {
      "label": "knows",
      "source": ["person_id"],
      "target": ["friend_id"],
      "input": {
        "type": "file",
        "path": "edges_knows.csv"
      }
    }
  ]
}
EOF

# 3. Load data into Store backend
cd hugegraph-loader
bin/hugegraph-loader.sh -g hugegraph -f load_config.json
```

---

### Cassandra/HBase to Store

**Use Case**: Migrating from legacy distributed backends

**Recommended Approach**: Backup & Restore

1. **Backup from old backend**:
   ```bash
   # With old backend configured
   bin/hugegraph-backup.sh --graph hugegraph --directory /backup/data
   ```

2. **Switch to Store backend** (reconfigure Server)

3. **Restore to Store**:
   ```bash
   # With Store backend configured
   bin/hugegraph-restore.sh --graph hugegraph --directory /backup/data
   ```

**Estimated Time**:
- 1 million vertices + 5 million edges: ~10-30 minutes
- 10 million vertices + 50 million edges: ~1-3 hours
- 100 million vertices + 500 million edges: ~10-30 hours

**Performance Tips**:
- Use `--batch-size 1000` for faster loading
- Run restore on a Server instance close to Store nodes (low latency)
- Temporarily increase `store.batch_size` during migration

---

## Multi-Graph Configuration

HugeGraph supports multiple graphs with different backends or configurations.

### Example: Multiple Graphs with Store

**Graph 1**: Main production graph
```properties
# conf/graphs/production.properties
backend=hstore
store.provider=org.apache.hugegraph.backend.store.hstore.HstoreProvider
store.pd_peers=192.168.1.10:8686,192.168.1.11:8686,192.168.1.12:8686
graph.name=production
```

**Graph 2**: Analytics graph
```properties
# conf/graphs/analytics.properties
backend=hstore
store.provider=org.apache.hugegraph.backend.store.hstore.HstoreProvider
store.pd_peers=192.168.1.10:8686,192.168.1.11:8686,192.168.1.12:8686
graph.name=analytics
```

**Access**:
```bash
# Production graph
curl http://localhost:8080/graphs/production/graph/vertices

# Analytics graph
curl http://localhost:8080/graphs/analytics/graph/vertices
```

### Mixed Backend Configuration

**Graph 1**: Store backend (distributed)
```properties
# conf/graphs/main.properties
backend=hstore
store.pd_peers=192.168.1.10:8686
graph.name=main
```

**Graph 2**: RocksDB backend (local)
```properties
# conf/graphs/local.properties
backend=rocksdb
rocksdb.data_path=./rocksdb-data
graph.name=local
```

---

## Troubleshooting Integration Issues

### Issue 1: Server Cannot Connect to PD

**Symptoms**:
```
ERROR o.a.h.b.s.h.HstoreProvider - Failed to connect to PD cluster
```

**Diagnosis**:
```bash
# Check PD is running
curl http://192.168.1.10:8620/actuator/health

# Check network connectivity
telnet 192.168.1.10 8686

# Check Server logs
tail -f logs/hugegraph-server.log | grep PD
```

**Solutions**:
1. Verify `store.pd_peers` addresses are correct
2. Ensure PD cluster is running and accessible
3. Check firewall rules (port 8686 must be open)
4. Try connecting to each PD peer individually

---

### Issue 2: Slow Query Performance

**Symptoms**:
- Queries take >5 seconds
- High latency in Server logs

**Diagnosis**:
```bash
# Check Store node health
curl http://192.168.1.20:8520/actuator/metrics

# Check partition distribution
curl http://192.168.1.10:8620/pd/v1/partitions

# Check if queries are using indexes
# (Enable query logging in Server)
```

**Solutions**:
1. **Create indexes**: Ensure label and property indexes exist
   ```groovy
   // In Gremlin console
   schema.indexLabel("personByName").onV("person").by("name").secondary().create()
   ```

2. **Increase Store nodes**: If data exceeds capacity of 3 nodes
3. **Tune RocksDB**: See [Best Practices](best-practices.md)
4. **Enable query pushdown**: Ensure Server is using Store's query API

---

### Issue 3: Write Failures

**Symptoms**:
```
ERROR o.a.h.b.s.h.HstoreSession - Write operation failed: Raft leader not found
```

**Diagnosis**:
```bash
# Check Store logs for Raft errors
tail -f logs/hugegraph-store.log | grep Raft

# Check partition leaders
curl http://192.168.1.10:8620/pd/v1/partitions | grep leader

# Check Store node states
curl http://192.168.1.10:8620/pd/v1/stores
```

**Solutions**:
1. **Wait for leader election**: If recent failover, wait 10-30 seconds
2. **Check Store node health**: Ensure all Store nodes are online
3. **Check disk space**: Ensure Store nodes have sufficient disk
4. **Restart affected Store node**: If Raft is stuck

---

### Issue 4: Data Inconsistency After Migration

**Symptoms**:
- Vertex/edge counts don't match
- Some data missing after restore

**Diagnosis**:
```bash
# Compare counts
curl http://localhost:8080/graphs/hugegraph/graph/vertices?limit=0
# vs expected count from backup

# Check for restore errors
tail -f logs/hugegraph-tools.log | grep ERROR
```

**Solutions**:
1. **Re-run restore**: Delete graph and restore again
   ```bash
   # Clear graph
   curl -X DELETE http://localhost:8080/graphs/hugegraph/graph/vertices

   # Restore
   bin/hugegraph-restore.sh --graph hugegraph --directory /backup/data
   ```

2. **Verify backup integrity**: Check backup files are complete
3. **Increase timeout**: If restore timed out, increase `store.rpc_timeout`

---

### Issue 5: Memory Leaks in Client

**Symptoms**:
- Server memory grows over time
- OutOfMemoryError after running for hours

**Diagnosis**:
```bash
# Monitor Server memory
jstat -gc <server-pid> 1000

# Heap dump analysis
jmap -dump:format=b,file=heap.bin <server-pid>
```

**Solutions**:
1. **Close sessions**: Ensure `HgStoreSession.close()` is called
   ```java
   try (HgStoreSession session = client.openSession(graphName)) {
       // Use session
   }  // Auto-closed
   ```

2. **Tune connection pool**: Reduce `store.max_sessions` if too high
3. **Increase heap**: Increase Server JVM heap size
   ```bash
   # In start-hugegraph.sh
   JAVA_OPTS="-Xms4g -Xmx8g"
   ```

---

For operational monitoring and troubleshooting, see [Operations Guide](operations-guide.md).

For performance optimization, see [Best Practices](best-practices.md).
