# REST Gremlin execution modes

`gremlinserver.rest_execution_mode` selects the execution path for JSON requests to
REST `GET /gremlin` and `POST /gremlin`. The default is `forward`.

```text
forward:  REST -> Jersey HTTP client -> Gremlin HTTP listener -> Gremlin workers
embedded: REST ---------------------------------------------> Gremlin workers
```

Both paths use the same Gremlin executor, graph manager, script engines and configured
JSON serializer. The Gremlin listener remains available to HTTP/WebSocket clients,
Driver and Console. Cypher and Gremlin jobs keep their existing execution paths.

## Configuration

In `conf/rest-server.properties`:

```properties
gremlinserver.url=http://127.0.0.1:8182
gremlinserver.rest_execution_mode=embedded
```

Restart the server to change modes. To revert, set the mode to `forward` and restart.
There is no automatic forwarding retry after an embedded execution failure: a query
may already have committed even when the caller receives an error.

The initial embedded implementation supports the standard HugeGraph
`WsAndHttpChannelizer` configuration, adapted internally to
`HugeGraphWsAndHttpChannelizer`. It requires the URL to identify the same process's
plain HTTP listener on a loopback address, with matching host/bind address and port.
Remote targets, TLS Gremlin listeners, custom channelizers and custom authorizers
continue to use `forward`.

Authentication must agree between REST and Gremlin: either both are anonymous, or
both use `StandardAuthenticator`, with `WsAndHttpBasicAuthHandler` on Gremlin and
`authentication.config.tokens` resolving to the same REST configuration file.
Unsupported combinations fail startup; they do not silently select another mode.

## Preserved behavior

- The REST endpoint remains JSON-only. It does not expose typed GraphSON or GraphBinary
  request formats, or introduce new content negotiation.
- GET keeps Jersey URI construction and the existing single-value rule for every query
  parameter. POST uses the TinkerPop 3.8.1 JSON request parser.
- Alias resolution uses the live graph manager. Request bindings override alias bindings.
- Evaluation, iterator materialization, commit and serialization stay on the same Gremlin
  worker, preserving thread-bound graph transactions and authentication context.
- Successful HTTP requests commit before serialization. A serialization failure does not
  imply rollback of an already committed write.
- Existing rollback hooks, evaluation timeout, error mapping and Gremlin metrics remain
  in use. REST input/output histograms remain at the same call sites; output length
  reflects the response entity metadata of the selected transport.
- The serializer instance is obtained from the initialized Channelizer. No second serializer
  registry, graph registry or script engine is created.

Embedded requests authenticate once at the REST entry point. Workers inherit the existing
HugeGraph user context without rechecking credentials. Requests accepted before token expiry
or a permission change retain the existing context semantics; subsequent requests follow the
existing authentication and cache behavior. This feature adds no cross-node revocation or
immutable authorization snapshot guarantee. Audit records use the original REST peer address;
forward audit records continue to use the internal HTTP client's address.

## Timeouts, load and lifecycle

`gremlinserver.max_route` is retained as blocking admission for embedded requests. Pending
callers wait rather than receive a new overload rejection. `gremlinserver.timeout` bounds
waiting for the result after admission, while the Gremlin executor retains its separate
execution timeout. The outer REST server's request timeout is unchanged.

A REST timeout does not cancel or retry the script. The script may continue and commit;
TinkerPop's existing execution timeout and rollback hooks remain responsible for execution.
Admission is released when the REST execution wait ends. This preserves the existing
possibility of work accumulating after client timeouts; it introduces no new hard task limit.

The adapter is unavailable until Gremlin startup has initialized its serializer. During
normal server shutdown it rejects new submissions before the existing Gremlin stop sequence.
The Gremlin server continues to own worker shutdown and graph closure. This feature adds no
new readiness endpoint, deployment probe, global drain deadline or stronger termination guarantee.

Protocol limits are checked against the internal request shape. This does not provide an
additional early limit before Jersey reads the outer REST request body. Existing transport
edge behavior is pinned by differential tests, including URI/header failures; it is not a new
listener-wide protection policy.

## Verification

Use Java 17 from the repository root:

```bash
mvn test -pl hugegraph-server/hugegraph-test -am -P unit-test \
  -Dtest=EmbeddedGremlinRestExecutorTest,HttpGremlinRequestHandlerTest,ContextGremlinServerHttpTest,ContextTaskTest,WsAndHttpBasicAuthHandlerTest,GremlinQueryAPITest \
  -Dsurefire.failIfNoSpecifiedTests=false
```

The focused tests compare real forward HTTP responses with embedded execution, exercise
both REST HTTP endpoints, and cover parsing/encoding, protocol limits, error mapping,
request identity, context isolation, blocking admission, timeouts, shared serializers,
dynamic aliases and graph transaction side effects on Memory and RocksDB. Response stack traces and newly generated
request IDs require semantic comparison; deterministic responses also use byte comparisons.

Run backend and distribution checks appropriate to the intended deployment before enabling
embedded. Focused tests do not establish HStore compatibility or production performance.
Default-mode changes require separate representative performance evidence.

Readiness, overload policy, authentication-cache consistency and global shutdown redesign
remain separate tasks from this execution-path change.

For a manual script-only HTTP transport comparison, run the focused tests above, then:

```bash
python3 hugegraph-server/hugegraph-test/scripts/gremlin-rest-benchmark.py \
  --output /tmp/gremlin-rest-benchmark.json
```

Set `JAVA_HOME` to Java 17. The runner uses the tested runtime classpath, starts two
local REST endpoints sharing one Gremlin worker pool, checks every measured response,
and alternates mode order across four rounds. It saves latency samples and environment
metadata. These measurements include client/REST overhead and exclude real backend I/O;
100 warmup requests do not establish steady-state JIT behavior.

For a Linux single-node RocksDB comparison, build the distribution and use an eight-CPU
host with Java 17. The following runner requires a new output directory and binds both
listeners to loopback. It generates its own graph and preserves all evidence under that
directory:

```bash
python3 hugegraph-server/hugegraph-test/scripts/gremlin-rest-rocksdb-benchmark.py \
  --distribution /path/to/apache-hugegraph-server-1.8.0 \
  --java-home /path/to/jdk-17 \
  --output /path/to/new-results-directory
```

It checks GET/POST, bindings, JSON values, aliases, rollback, committed write counts and
restart persistence. The default dataset has 10,000 vertices and 50,000 edges. Four server
starts use forward/embedded/embedded/forward order, identical JVM settings and persistent
data. Each workload has a five-second warmup and three-second measurements at concurrency
1, 8 and 32. Samples use a closed-loop client on the same host; CPU affinity separates
the client and server, while unrelated host workloads can still affect both. The results
include raw latency samples, response checks, CPU/load snapshots, GC and server logs.
These short measurements do not establish production saturation or steady-state behavior.
