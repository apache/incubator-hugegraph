package com.baidu.hugegraph.backend.store.cassandra;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.configuration.ConfigSpace;
import com.baidu.hugegraph.configuration.HugeConfiguration;
import com.baidu.hugegraph.type.HugeTypes;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.google.common.base.Preconditions;

public class CassandraStore implements BackendStore {

    private static final Logger logger = LoggerFactory.getLogger(CassandraStore.class);

    private final String name;
    private Cluster cluster;
    private Session session;

    private Map<HugeTypes, CassandraTable> tables = null;

    private HugeConfiguration config = null;

    public CassandraStore(final String name) {
        this.name = name;
        this.tables = new ConcurrentHashMap<>();

        this.initTableManagers();

        logger.debug("Store loaded: {}", name);
    }

    private void initTableManagers() {
        this.tables.put(HugeTypes.VERTEX_LABEL, new CassandraTable.VertexLabel());
        this.tables.put(HugeTypes.EDGE_LABEL, new CassandraTable.EdgeLabel());
        this.tables.put(HugeTypes.PROPERTY_KEY, new CassandraTable.PropertyKey());
        this.tables.put(HugeTypes.INDEX_LABEL, new CassandraTable.IndexLabel());

        this.tables.put(HugeTypes.VERTEX, new CassandraTable.Vertex());
        this.tables.put(HugeTypes.EDGE, new CassandraTable.Edge());
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public void open(HugeConfiguration config) {
        assert config != null;
        this.config  = config;

        String hosts = config.get(ConfigSpace.CASSANDRA_HOST);
        int port = config.get(ConfigSpace.CASSANDRA_PORT);
        String keyspace = config.get(ConfigSpace.CASSANDRA_KEYSPACE);

        // init cluster
        this.cluster = Cluster.builder().addContactPoints(
                hosts.split(",")).withPort(port).build();

        // init session
        try {
            try {
                logger.debug("Store connect with keyspace: {}", keyspace);
                this.session = this.cluster.connect(keyspace);
            } catch (InvalidQueryException e) {
                // TODO: the error message may be changed in different versions
                if (!e.getMessage().contains(String.format(
                        "Keyspace '%s' does not exist", keyspace))) {
                    throw e;
                }
                logger.info("Failed to connect keyspace: {},"
                        + " try connect without keyspace", keyspace);
                this.session = this.cluster.connect();
            }
        } catch (Exception e) {
            this.cluster.close();
        }

        logger.debug("Store opened: {}", this.name);
    }

    @Override
    public void close() {
        try {
            this.session.close();
        } finally {
            this.cluster.close();
        }

        logger.debug("Store closed: {}", this.name);
    }

    @Override
    public void mutate(BackendMutation mutation) {
        logger.debug("Store mutate: additions={}, deletions={}",
                mutation.additions().size(),
                mutation.deletions().size());

        this.checkConneted();

        // delete data
        for (BackendEntry i : mutation.deletions()) {
            CassandraBackendEntry entry = castBackendEntry(i);
            // delete entry
            this.table(entry.type()).delete(entry.row());
            // delete sub rows (edges)
            for (CassandraBackendEntry.Row row : entry.subRows()) {
                this.table(row.type()).delete(row);
            }
        }

        // insert data
        for (BackendEntry i : mutation.additions()) {
            CassandraBackendEntry entry = castBackendEntry(i);
            // insert entry
            this.table(entry.type()).insert(entry.row());
            // insert sub rows (edges)
            for (CassandraBackendEntry.Row row : entry.subRows()) {
                this.table(row.type()).insert(row);
            }
        }
    }

    @Override
    public Iterable<BackendEntry> query(Query query) {
        logger.debug("Store query: {}", query);
        this.checkConneted();
        return this.table(query.resultType()).query(this.session, query);
    }

    @Override
    public void init() {
        this.checkConneted();

        this.initKeyspace();
        this.initTables();

        logger.info("Store initialized: {}", this.name);
    }

    @Override
    public void clear() {
        this.checkConneted();

        if (this.existsKeyspace()) {
            this.clearTables();
            this.clearKeyspace();
        }

        logger.info("Store cleared: {}", this.name);
    }

    @Override
    public void beginTx() {
        // TODO how to implement?
    }

    @Override
    public void commitTx() {
        // do update
        logger.debug("Store commit: {}", this.name);
        for (CassandraTable i : this.tables.values()) {
            if (i.hasChanged()) {
                i.commit(this.session);
            }
        }
        logger.debug("Store commited: {}", this.name);

        // TODO how to implement tx?
    }

    @Override
    public void rollbackTx() {
        // TODO how to implement?
    }

    protected void initKeyspace() {
        // keyspace
        String keyspace = this.config.get(ConfigSpace.CASSANDRA_KEYSPACE);

        // replication strategy: SimpleStrategy or NetworkTopologyStrategy
        String strategy = this.config.get(ConfigSpace.CASSANDRA_STRATEGY);

        // replication factor
        int replication = this.config.get(ConfigSpace.CASSANDRA_REPLICATION);

        String cql = String.format("CREATE KEYSPACE IF NOT EXISTS %s "
                + "WITH replication={'class':'%s', 'replication_factor':%d}",
                keyspace,
                strategy, replication);

        logger.info("Create keyspace : {}", cql);
        this.session.execute(cql);

        if (!this.session.isClosed()) {
            this.session.close();
        }
        this.session = this.cluster.connect(keyspace);
    }

    protected void clearKeyspace() {
        String keyspace = this.config.get(ConfigSpace.CASSANDRA_KEYSPACE);
        logger.info("Drop keyspace : {}", keyspace);

        if (!this.session.isClosed()) {
            this.session.close();
        }
        this.session = this.cluster.connect();
        this.session.execute(SchemaBuilder.dropKeyspace(keyspace).ifExists());
    }

    protected boolean existsKeyspace() {
        String keyspace = this.config.get(ConfigSpace.CASSANDRA_KEYSPACE);
        return this.cluster.getMetadata().getKeyspace(keyspace) != null;
    }

    protected void initTables() {
        for (CassandraTable t : this.tables.values()) {
            t.init(this.session);
        }
    }

    protected void clearTables() {
        for (CassandraTable t : this.tables.values()) {
            t.clear(this.session);
        }
    }

    protected CassandraTable table(HugeTypes type) {
        assert type != null;
        CassandraTable t = this.tables.get(type);
        if (t == null) {
            throw new BackendException("Not supported type:" + type.name());
        }
        return t;
    }


    protected void checkConneted() {
        Preconditions.checkNotNull(this.cluster,
                "Cassandra cluster has not been initialized");
        Preconditions.checkState(!this.cluster.isClosed(),
                "Cassandra cluster has been closed");

        Preconditions.checkNotNull(this.session,
                "Cassandra session has not been initialized");
        Preconditions.checkState(!this.session.isClosed(),
                "Cassandra session has been closed");
    }

    protected static CassandraBackendEntry castBackendEntry(BackendEntry entry) {
        assert entry instanceof CassandraBackendEntry;
        if (!(entry instanceof CassandraBackendEntry)) {
            throw new BackendException(
                    "Cassandra store only supports CassandraBackendEntry");
        }
        return (CassandraBackendEntry) entry;
    }
}
