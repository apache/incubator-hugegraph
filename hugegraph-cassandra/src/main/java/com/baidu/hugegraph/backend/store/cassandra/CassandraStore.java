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
import com.baidu.hugegraph.config.CassandraOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.type.HugeType;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.google.common.base.Preconditions;

public abstract class CassandraStore implements BackendStore {

    private static final Logger logger = LoggerFactory.getLogger(
            CassandraStore.class);

    private final String name;
    private final String keyspace;
    private CassandraSessionPool sessions;

    private Map<HugeType, CassandraTable> tables = null;

    private HugeConfig conf = null;

    public CassandraStore(final String keyspace, final String name) {
        Preconditions.checkNotNull(keyspace);
        Preconditions.checkNotNull(name);

        this.keyspace = keyspace;
        this.name = name;
        this.sessions = new CassandraSessionPool(this.keyspace);
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
    public void open(HugeConfig config) {
        if (this.sessions.isOpened()) {
            // TODO: maybe we should throw an exception here instead of ignore
            logger.debug("Store {} has been opened before", this.name);
            return;
        }
        assert config != null;
        this.conf = config;

        String hosts = config.get(CassandraOptions.CASSANDRA_HOST);
        int port = config.get(CassandraOptions.CASSANDRA_PORT);

        // init cluster
        this.sessions.open(hosts, port);

        // init a session for current thread
        try {
            logger.debug("Store connect with keyspace: {}", this.keyspace);
            try {
                this.sessions.session();
            } catch (InvalidQueryException e) {
                // TODO: the error message may be changed in different versions
                if (!e.getMessage().contains(String.format(
                        "Keyspace '%s' does not exist", this.keyspace))) {
                    throw e;
                }
                logger.info("Failed to connect keyspace: {},"
                        + " try init keyspace later", this.keyspace);
                this.sessions.closeSession();
            }
        } catch (Exception e) {
            try {
                this.sessions.close();
            } catch (Exception e2) {
                logger.warn("Failed to close cluster", e2);
            }
            throw e;
        }

        logger.debug("Store opened: {}", this.name);
    }

    @Override
    public void close() {
        this.sessions.close();
        logger.debug("Store closed: {}", this.name);
    }

    @Override
    public void mutate(BackendMutation mutation) {
        logger.debug("Store {} mutate: additions={}, deletions={}",
                this.name,
                mutation.additions().size(),
                mutation.deletions().size());

        this.checkSessionConneted();
        CassandraSessionPool.Session session = this.sessions.session();

        // delete data
        for (BackendEntry i : mutation.deletions()) {
            CassandraBackendEntry entry = castBackendEntry(i);
            if (entry.selfChanged()) {
                // delete entry
                this.table(entry.type()).delete(session, entry.row());
            }
            // delete sub rows (edges)
            for (CassandraBackendEntry.Row row : entry.subRows()) {
                this.table(row.type()).delete(session, row);
            }
        }

        // insert data
        for (BackendEntry i : mutation.additions()) {
            CassandraBackendEntry entry = castBackendEntry(i);
            // insert entry
            if (entry.selfChanged()) {
                this.table(entry.type()).insert(session, entry.row());
            }
            // insert sub rows (edges)
            for (CassandraBackendEntry.Row row : entry.subRows()) {
                this.table(row.type()).insert(session, row);
            }
        }
    }

    @Override
    public Iterable<BackendEntry> query(Query query) {
        this.checkSessionConneted();

        CassandraTable table = this.table(query.resultType());
        return table.query(this.sessions.session(), query);
    }

    @Override
    public Object metadata(HugeType type, String meta, Object[] args) {
        this.checkSessionConneted();

        CassandraTable table = this.table(type);
        return table.metadata(this.sessions.session(), meta, args);
    }

    @Override
    public void init() {
        this.checkClusterConneted();

        this.initKeyspace();
        this.initTables();

        logger.info("Store initialized: {}", this.name);
    }

    @Override
    public void clear() {
        this.checkClusterConneted();

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
        this.checkSessionConneted();

        // do update
        CassandraSessionPool.Session session = this.sessions.session();
        if (!session.hasChanged()) {
            logger.debug("Store {} has nothing to commit", this.name);
            return;
        }

        logger.debug("Store {} commit statements: {}",
                this.name, session.statements());
        try {
            session.commit();
        } catch (InvalidQueryException e) {
            logger.error("Failed to commit statements due to:", e);
            assert session.statements().size() > 0;
            throw new BackendException(
                    "Failed to commit %s statements: '%s'...",
                    session.statements().size(),
                    session.statements().iterator().next());
        } finally {
            session.clear();
        }

        // TODO how to implement tx?
    }

    @Override
    public void rollbackTx() {
        // TODO how to implement?
        throw new UnsupportedOperationException(
                "Unsupported rollback operation by Cassandra");
    }

    @Override
    public String toString() {
        return this.name;
    }

    protected Cluster cluster() {
        return this.sessions.cluster();
    }

    protected void initKeyspace() {
        // replication strategy: SimpleStrategy or NetworkTopologyStrategy
        String strategy = this.conf.get(CassandraOptions.CASSANDRA_STRATEGY);

        // replication factor
        int replication = this.conf.get(CassandraOptions.CASSANDRA_REPLICATION);

        String cql = String.format("CREATE KEYSPACE IF NOT EXISTS %s "
                + "WITH replication={'class':'%s', 'replication_factor':%d}",
                this.keyspace,
                strategy, replication);

        // create keyspace with non-keyspace-session
        logger.info("Create keyspace: {}", cql);
        Session session = this.cluster().connect();
        try {
            session.execute(cql);
        } finally {
            if (!session.isClosed()) {
                session.close();
            }
        }
    }

    protected void clearKeyspace() {
        // drop keyspace with non-keyspace-session
        Statement stmt = SchemaBuilder.dropKeyspace(this.keyspace).ifExists();
        logger.info("Drop keyspace: {}", stmt);

        Session session = this.cluster().connect();
        try {
            session.execute(stmt);
        } finally {
            if (!session.isClosed()) {
                session.close();
            }
        }
    }

    protected boolean existsKeyspace() {
        return this.cluster().getMetadata().getKeyspace(this.keyspace) != null;
    }

    protected void initTables() {
        CassandraSessionPool.Session session = this.sessions.session();
        for (CassandraTable table : this.tables.values()) {
            table.init(session);
        }
    }

    protected void clearTables() {
        CassandraSessionPool.Session session = this.sessions.session();
        for (CassandraTable table : this.tables.values()) {
            table.clear(session);
        }
    }

    protected CassandraTable table(HugeType type) {
        assert type != null;
        CassandraTable table = this.tables.get(type);
        if (table == null) {
            throw new BackendException("Unsupported type:" + type.name());
        }
        return table;
    }

    protected void checkClusterConneted() {
        Preconditions.checkNotNull(this.sessions,
                "Cassandra store has not been initialized");
        this.sessions.checkClusterConneted();
    }

    protected void checkSessionConneted() {
        Preconditions.checkNotNull(this.sessions,
                "Cassandra store has not been initialized");
        this.sessions.checkSessionConneted();
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
            super.tables.put(HugeType.VERTEX_LABEL, new CassandraTables.VertexLabel());
            super.tables.put(HugeType.EDGE_LABEL, new CassandraTables.EdgeLabel());
            super.tables.put(HugeType.PROPERTY_KEY, new CassandraTables.PropertyKey());
            super.tables.put(HugeType.INDEX_LABEL, new CassandraTables.IndexLabel());
        }
    }

    public static class CassandraGraphStore extends CassandraStore {

        public CassandraGraphStore(String keyspace, String name) {
            super(keyspace, name);
        }

        @Override
        protected void initTableManagers() {
            super.tables.put(HugeType.VERTEX, new CassandraTables.Vertex());
            super.tables.put(HugeType.EDGE, new CassandraTables.Edge());
        }
    }

    public static class CassandraIndexStore extends CassandraStore {

        public CassandraIndexStore(String keyspace, String name) {
            super(keyspace, name);
        }

        @Override
        protected void initTableManagers() {
            super.tables.put(HugeType.SECONDARY_INDEX, new CassandraTables.SecondaryIndex());
            super.tables.put(HugeType.SEARCH_INDEX, new CassandraTables.SearchIndex());
        }
    }
}
