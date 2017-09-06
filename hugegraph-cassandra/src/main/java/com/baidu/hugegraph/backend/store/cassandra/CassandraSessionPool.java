/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.backend.store.cassandra;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;

public class CassandraSessionPool {

    private static final Logger LOG = Log.logger(CassandraStore.class);

    private Cluster cluster;
    private String keyspace;

    private ThreadLocal<Session> threadLocalSession;
    private AtomicInteger sessionCount;

    public CassandraSessionPool(String keyspace) {
        this.cluster = null;
        this.keyspace = keyspace;

        this.threadLocalSession = new ThreadLocal<>();
        this.sessionCount = new AtomicInteger(0);
    }

    public synchronized void open(String hosts, int port) {
        if (this.opened()) {
            throw new BackendException("Please close the old SessionPool " +
                                       "before opening a new one");
        }
        assert this.cluster == null || this.cluster.isClosed();
        this.cluster = Cluster.builder()
                       .addContactPoints(hosts.split(","))
                       .withPort(port)
                       .build();
    }

    public final synchronized boolean opened() {
        return (this.cluster != null && !this.cluster.isClosed());
    }

    public final synchronized Cluster cluster() {
        E.checkState(this.cluster != null,
                     "Cassandra cluster has not been initialized");
        return this.cluster;
    }

    public final synchronized Session session() {
        Session session = this.threadLocalSession.get();
        if (session == null) {
            E.checkState(this.cluster != null,
                         "Cassandra cluster has not been initialized");
            session = new Session(this.cluster.connect(this.keyspace));
            this.threadLocalSession.set(session);
            this.sessionCount.incrementAndGet();
            LOG.debug("Now(after connect()) session count is: {}",
                      this.sessionCount.get());
        }
        return session;
    }

    public void useSession() {
        Session session = this.threadLocalSession.get();
        if (session == null) {
            return;
        }
        session.attach();
    }

    public void closeSession() {
        Session session = this.threadLocalSession.get();
        if (session == null) {
            return;
        }
        if (session.detach() <= 0) {
            session.close();
            this.threadLocalSession.remove();
            this.sessionCount.decrementAndGet();
        }
    }

    public synchronized void close() {
        try {
            this.closeSession();
        } finally {
            if (this.sessionCount.get() == 0 &&
                this.cluster != null &&
                !this.cluster.isClosed()) {
                this.cluster.close();
            }
        }
        LOG.debug("Now(after close()) session count is: {}",
                  this.sessionCount.get());
    }

    public final void checkClusterConnected() {
        E.checkState(this.cluster != null,
                     "Cassandra cluster has not been initialized");
        E.checkState(!this.cluster.isClosed(),
                     "Cassandra cluster has been closed");
    }

    public final void checkSessionConnected() {
        this.checkClusterConnected();

        E.checkState(this.session() != null,
                     "Cassandra session has not been initialized");
        E.checkState(!this.session().closed(),
                     "Cassandra session has been closed");
    }

    /**
     * The Session class is a wrapper of driver Session
     * Expect every thread hold a its own session(wrapper)
     */
    protected final class Session {

        private com.datastax.driver.core.Session session;
        private BatchStatement batch;
        private int refs;

        public Session(com.datastax.driver.core.Session session) {
            this.session = session;
            this.batch = new BatchStatement();
            this.refs = 1;
        }

        private int attach() {
            return ++this.refs;
        }

        private int detach() {
            return --this.refs;
        }

        public BatchStatement add(Statement statement) {
            return this.batch.add(statement);
        }

        public void clear() {
            this.batch.clear();
        }

        public ResultSet commit() {
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

        public boolean closed() {
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
