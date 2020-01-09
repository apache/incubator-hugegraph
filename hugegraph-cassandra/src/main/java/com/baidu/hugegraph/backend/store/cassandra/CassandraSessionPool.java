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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.store.BackendSession;
import com.baidu.hugegraph.backend.store.BackendSessionPool;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.E;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.InvalidQueryException;

public class CassandraSessionPool extends BackendSessionPool {

    private static final int SECOND = 1000;

    private Cluster cluster;
    private String keyspace;

    public CassandraSessionPool(HugeConfig config, String keyspace,
                                String store) {
        super(config, keyspace + "/" + store);
        this.cluster = null;
        this.keyspace = keyspace;
    }

    @Override
    public synchronized void open() {
        if (this.opened()) {
            throw new BackendException("Please close the old SessionPool " +
                                       "before opening a new one");
        }

        HugeConfig config = this.config();
        // Contact options
        String hosts = config.get(CassandraOptions.CASSANDRA_HOST);
        int port = config.get(CassandraOptions.CASSANDRA_PORT);

        assert this.cluster == null || this.cluster.isClosed();
        Builder builder = Cluster.builder()
                                 .addContactPoints(hosts.split(","))
                                 .withPort(port);

        // Timeout options
        int connTimeout = config.get(CassandraOptions.CASSANDRA_CONN_TIMEOUT);
        int readTimeout = config.get(CassandraOptions.CASSANDRA_READ_TIMEOUT);

        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setConnectTimeoutMillis(connTimeout * SECOND);
        socketOptions.setReadTimeoutMillis(readTimeout * SECOND);

        builder.withSocketOptions(socketOptions);

        // Credential options
        String username = config.get(CassandraOptions.CASSANDRA_USERNAME);
        String password = config.get(CassandraOptions.CASSANDRA_PASSWORD);
        if (!username.isEmpty()) {
            builder.withCredentials(username, password);
        }

        // Compression options
        String compression = config.get(CassandraOptions.CASSANDRA_COMPRESSION);
        builder.withCompression(Compression.valueOf(compression.toUpperCase()));

        this.cluster = builder.build();
    }

    @Override
    public final synchronized boolean opened() {
        return (this.cluster != null && !this.cluster.isClosed());
    }

    protected final synchronized Cluster cluster() {
        E.checkState(this.cluster != null,
                     "Cassandra cluster has not been initialized");
        return this.cluster;
    }

    @Override
    public final Session session() {
        return (Session) super.getOrNewSession();
    }

    @Override
    protected Session newSession() {
        E.checkState(this.cluster != null,
                     "Cassandra cluster has not been initialized");
        return new Session();
    }

    @Override
    protected synchronized void doClose() {
        if (this.cluster != null && !this.cluster.isClosed()) {
            this.cluster.close();
        }
    }

    public final boolean clusterConnected() {
        E.checkState(this.cluster != null,
                     "Cassandra cluster has not been initialized");
        return !this.cluster.isClosed();
    }

    /**
     * The Session class is a wrapper of driver Session
     * Expect every thread hold a its own session(wrapper)
     */
    public final class Session extends BackendSession {

        private com.datastax.driver.core.Session session;
        private BatchStatement batch;

        public Session() {
            this.session = null;
            this.batch = new BatchStatement(); // LOGGED
        }

        public BatchStatement add(Statement statement) {
            return this.batch.add(statement);
        }

        @Override
        public void rollback() {
            this.batch.clear();
        }

        @Override
        public ResultSet commit() {
            ResultSet rs = this.session.execute(this.batch);
            // Clear batch if execute() successfully (retained if failed)
            this.batch.clear();
            return rs;
        }

        public void commitAsync() {
            Collection<Statement> statements = this.batch.getStatements();

            int count = 0;
            int processors = Math.min(statements.size(), 1023);
            List<ResultSetFuture> results = new ArrayList<>(processors + 1);
            for (Statement s : statements) {
                ResultSetFuture future = this.session.executeAsync(s);
                results.add(future);

                if (++count > processors) {
                    results.forEach(ResultSetFuture::getUninterruptibly);
                    results.clear();
                    count = 0;
                }
            }
            for (ResultSetFuture future : results) {
                future.getUninterruptibly();
            }

            // Clear batch if execute() successfully (retained if failed)
            this.batch.clear();
        }

        public ResultSet query(Statement statement) {
            assert !this.hasChanges();
            return this.execute(statement);
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

        private void tryOpen() {
            assert this.session == null;
            try {
                this.open();
            } catch (InvalidQueryException ignored) {}
        }

        @Override
        public void open() {
            assert this.session == null;
            this.session = cluster().connect(keyspace());
            this.opened = true;
        }

        @Override
        public boolean opened() {
            if (this.opened && this.session == null) {
                this.tryOpen();
            }
            return this.opened && this.session != null;
        }

        @Override
        public boolean closed() {
            if (!this.opened || this.session == null) {
                return true;
            }
            return this.session.isClosed();
        }

        @Override
        public void close() {
            assert this.closeable();
            if (this.session == null) {
                return;
            }
            this.session.close();
            this.session = null;
        }

        @Override
        public boolean hasChanges() {
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
