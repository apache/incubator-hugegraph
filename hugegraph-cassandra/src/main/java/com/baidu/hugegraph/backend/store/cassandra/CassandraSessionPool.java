package com.baidu.hugegraph.backend.store.cassandra;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.backend.BackendException;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.google.common.base.Preconditions;

public class CassandraSessionPool {

    private static final Logger logger = LoggerFactory.getLogger(
            CassandraStore.class);

    private Cluster cluster;
    private String keyspace;

    private ThreadLocal<Session> threadLocalSession;
    private AtomicInteger sessionCount;

    public CassandraSessionPool(String keyspace) {
        this.keyspace = keyspace;

        this.threadLocalSession = new ThreadLocal<>();
        this.sessionCount = new AtomicInteger(0);
    }

    public void open(String hosts, int port) {
        if (isOpened()) {
            throw new BackendException("Please close the old SessionPool "
                    + "before open a new one");
        }
        this.cluster = Cluster.builder()
                .addContactPoints(hosts.split(","))
                .withPort(port)
                .build();
    }

    public boolean isOpened() {
        return (this.cluster != null && !this.cluster.isClosed());
    }

    public Cluster cluster() {
        return this.cluster;
    }

    public synchronized Session session() {
        Session session = this.threadLocalSession.get();
        if (session == null) {
            session = new Session(this.cluster.connect(this.keyspace));
            this.threadLocalSession.set(session);
            this.sessionCount.incrementAndGet();
            logger.debug("Now(after connect()) session count is: {}",
                    this.sessionCount.get());
        }
        return session;
    }

    public void closeSession() {
        Session session = this.threadLocalSession.get();
        if (session == null) {
            return;
        }
        session.close();
        this.threadLocalSession.remove();
        this.sessionCount.decrementAndGet();
    }

    public void close() {
        try {
            this.closeSession();
        } finally {
            if (this.sessionCount.get() == 0 && !this.cluster.isClosed()) {
                this.cluster.close();
            }
        }
        logger.debug("Now(after close()) session count is: {}",
                this.sessionCount.get());
    }

    public void checkClusterConneted() {
        Preconditions.checkNotNull(this.cluster,
                "Cassandra cluster has not been initialized");
        Preconditions.checkState(!this.cluster.isClosed(),
                "Cassandra cluster has been closed");
    }

    public void checkSessionConneted() {
        this.checkClusterConneted();

        Preconditions.checkNotNull(this.session(),
                "Cassandra session has not been initialized");
        Preconditions.checkState(!this.session().isClosed(),
                "Cassandra session has been closed");
    }

    // Expect every thread hold a Session wrapper
    class Session {

        private com.datastax.driver.core.Session session;
        private BatchStatement batch;

        public Session(com.datastax.driver.core.Session session) {
            this.session = session;
            this.batch = new BatchStatement();
        }

        public BatchStatement add(Statement statement) {
            return this.batch.add(statement);
        }

        public void clear() {
            this.batch.clear();
        }

        public synchronized ResultSet commit() {
            return this.session.execute(this.batch);
        }

        public ResultSet execute(Statement statement) {
            return this.session.execute(statement);
        }

        public ResultSet execute(String statement) {
            return this.session.execute(statement);
        }

        public ResultSet execute(String statement, Object... args) {
            return this.session.execute(statement, args);
        }

        public boolean isClosed() {
            return this.session.isClosed();
        }

        private void close() {
            this.session.close();
        }

        public boolean hasChanged() {
            return this.batch.size() > 0;
        }

        public Collection<Statement> statements() {
            return this.batch.getStatements();
        }

        public String keyspace() {
            return CassandraSessionPool.this.keyspace;
        }

        public Metadata metadata() {
            return CassandraSessionPool.this.cluster.getMetadata();
        }
    }
}
