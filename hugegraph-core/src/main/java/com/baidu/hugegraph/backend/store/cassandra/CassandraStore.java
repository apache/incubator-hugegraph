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

public abstract class CassandraStore implements BackendStore {

    private static final Logger logger = LoggerFactory.getLogger(CassandraStore.class);

    private final String keyspace;
    private final String name;
    private Cluster cluster;
    private Session session;

    private Map<HugeTypes, CassandraTable> tables = null;

    private HugeConfiguration config = null;

    public CassandraStore(final String keyspace, final String name) {
        Preconditions.checkNotNull(keyspace);
        Preconditions.checkNotNull(name);

        this.keyspace = keyspace;
        this.name = name;
        this.tables = new ConcurrentHashMap<>();

        this.initTableManagers();

        logger.debug("Store loaded: {}", name);
    }

    protected abstract void initTableManagers();

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

        // init cluster
        this.cluster = Cluster.builder().addContactPoints(
                hosts.split(",")).withPort(port).build();

        // init session
        try {
            try {
                logger.debug("Store connect with keyspace: {}", this.keyspace);
                this.session = this.cluster.connect(this.keyspace);
            } catch (InvalidQueryException e) {
                // TODO: the error message may be changed in different versions
                if (!e.getMessage().contains(String.format(
                        "Keyspace '%s' does not exist", this.keyspace))) {
                    throw e;
                }
                logger.info("Failed to connect keyspace: {},"
                        + " try connect without keyspace", this.keyspace);
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
        logger.debug("Store {} mutate: additions={}, deletions={}",
                this.name,
                mutation.additions().size(),
                mutation.deletions().size());

        this.checkConneted();

        // delete data
        for (BackendEntry i : mutation.deletions()) {
            CassandraBackendEntry entry = castBackendEntry(i);
            // NOTE: we assume that not delete entry if subRows exist
            if (entry.subRows().isEmpty()) {
                // delete entry
                this.table(entry.type()).delete(entry.row());
            } else {
                // assert entry.row().keys().isEmpty(); // only contains id
                assert entry.row().cells().isEmpty();
                // delete sub rows (edges)
                for (CassandraBackendEntry.Row row : entry.subRows()) {
                    this.table(row.type()).delete(row);
                }
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

    @Override
    public String toString() {
        return this.name;
    }

    protected void initKeyspace() {
        // replication strategy: SimpleStrategy or NetworkTopologyStrategy
        String strategy = this.config.get(ConfigSpace.CASSANDRA_STRATEGY);

        // replication factor
        int replication = this.config.get(ConfigSpace.CASSANDRA_REPLICATION);

        String cql = String.format("CREATE KEYSPACE IF NOT EXISTS %s "
                + "WITH replication={'class':'%s', 'replication_factor':%d}",
                this.keyspace,
                strategy, replication);

        logger.info("Create keyspace : {}", cql);
        this.session.execute(cql);

        if (!this.session.isClosed()) {
            this.session.close();
        }
        this.session = this.cluster.connect(this.keyspace);
    }

    protected void clearKeyspace() {
        logger.info("Drop keyspace : {}", this.keyspace);

        if (!this.session.isClosed()) {
            this.session.close();
        }
        this.session = this.cluster.connect();
        this.session.execute(SchemaBuilder.dropKeyspace(this.keyspace).ifExists());
    }

    protected boolean existsKeyspace() {
        return this.cluster.getMetadata().getKeyspace(this.keyspace) != null;
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

    /***************************** store defines *****************************/

    public static class CassandraSchemaStore extends CassandraStore {

        public CassandraSchemaStore(String keyspace, String name) {
            super(keyspace, name);
        }

        @Override
        protected void initTableManagers() {
            super.tables.put(HugeTypes.VERTEX_LABEL, new CassandraTables.VertexLabel());
            super.tables.put(HugeTypes.EDGE_LABEL, new CassandraTables.EdgeLabel());
            super.tables.put(HugeTypes.PROPERTY_KEY, new CassandraTables.PropertyKey());
            super.tables.put(HugeTypes.INDEX_LABEL, new CassandraTables.IndexLabel());
        }
    }

    public static class CassandraGraphStore extends CassandraStore {

        public CassandraGraphStore(String keyspace, String name) {
            super(keyspace, name);
        }

        @Override
        protected void initTableManagers() {
            super.tables.put(HugeTypes.VERTEX, new CassandraTables.Vertex());
            super.tables.put(HugeTypes.EDGE, new CassandraTables.Edge());
        }
    }

    public static class CassandraIndexStore extends CassandraStore {

        public CassandraIndexStore(String keyspace, String name) {
            super(keyspace, name);
        }

        @Override
        protected void initTableManagers() {
            super.tables.put(HugeTypes.SECONDARY_INDEX, new CassandraTables.SecondaryIndex());
            super.tables.put(HugeTypes.SEARCH_INDEX, new CassandraTables.SearchIndex());
        }
    }
}
