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

package com.baidu.hugegraph.backend.store.mysql;

import java.net.SocketTimeoutException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.store.BackendSession;
import com.baidu.hugegraph.backend.store.BackendSessionPool;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.Log;

public class MysqlSessions extends BackendSessionPool {

    private static final Logger LOG = Log.logger(MysqlStore.class);

    private static final int DROP_DB_TIMEOUT = 10000;

    private HugeConfig config;
    private String database;
    private volatile boolean opened;

    public MysqlSessions(HugeConfig config, String database, String store) {
        super(config, database + "/" + store);
        this.config = config;
        this.database = database;
        this.opened = false;
    }

    @Override
    public HugeConfig config() {
        return this.config;
    }

    public String database() {
        return this.database;
    }

    public String escapedDatabase() {
        return MysqlUtil.escapeString(this.database());
    }

    /**
     * Try connect with specified database, will not reconnect if failed
     * @throws SQLException if a database access error occurs
     */
    @Override
    public synchronized void open() throws Exception {
        try (Connection conn = this.open(false)) {
            this.opened = true;
        }
    }

    @Override
    protected boolean opened() {
        return this.opened;
    }

    @Override
    protected void doClose() {
        // pass
    }

    @Override
    public Session session() {
        return (Session) super.getOrNewSession();
    }

    @Override
    protected Session newSession() {
        return new Session();
    }

    public void createDatabase() {
        // Create database with non-database-session
        LOG.debug("Create database: {}", this.database());

        String sql = this.buildCreateDatabase(this.database());
        try (Connection conn = this.openWithoutDB(0)) {
            conn.createStatement().execute(sql);
        } catch (SQLException e) {
            if (!e.getMessage().endsWith("already exists")) {
                throw new BackendException("Failed to create database '%s'", e,
                                           this.database());
            }
            // Ignore exception if database already exists
        }
    }

    public void dropDatabase() {
        LOG.debug("Drop database: {}", this.database());

        // Close the under layer connections owned by each thread
        this.forceResetSessions();

        String sql = this.buildDropDatabase(this.database());
        try (Connection conn = this.openWithoutDB(DROP_DB_TIMEOUT)) {
            conn.createStatement().execute(sql);
        } catch (SQLException e) {
            if (e.getCause() instanceof SocketTimeoutException) {
                LOG.warn("Drop database '{}' timeout", this.database());
            } else {
                throw new BackendException("Failed to drop database '%s'", e,
                                           this.database());
            }
        }
    }

    public boolean existsDatabase() {
        try (Connection conn = this.openWithoutDB(0);
             ResultSet result = conn.getMetaData().getCatalogs()) {
            while (result.next()) {
                String dbName = result.getString(1);
                if (dbName.equals(this.database())) {
                    return true;
                }
            }
        } catch (Exception e) {
            throw new BackendException("Failed to obtain database info", e);
        }
        return false;
    }

    public boolean existsTable(String table) {
        String sql = this.buildExistsTable(table);
        try (Connection conn = this.openWithDB(0);
             ResultSet result = conn.createStatement().executeQuery(sql)) {
            return result.next();
        } catch (Exception e) {
            throw new BackendException("Failed to obtain table info", e);
        }
    }

    protected String buildCreateDatabase(String database) {
        return String.format("CREATE DATABASE IF NOT EXISTS %s " +
                             "DEFAULT CHARSET utf8 COLLATE utf8_bin;",
                             database);
    }

    protected String buildDropDatabase(String database) {
        return String.format("DROP DATABASE IF EXISTS %s;", database);
    }

    protected String buildExistsTable(String table) {
        return String.format("SELECT * FROM information_schema.tables " +
                             "WHERE table_schema = '%s' " +
                             "AND table_name = '%s' LIMIT 1;",
                             this.escapedDatabase(),
                             MysqlUtil.escapeString(table));
    }

    /**
     * Connect DB without specified database
     */
    protected Connection openWithoutDB(int timeout) {
        String jdbcUrl = this.config.get(MysqlOptions.JDBC_URL);
        String url = new URIBuilder().setPath(jdbcUrl)
                                     .setParameter("socketTimeout",
                                                   String.valueOf(timeout))
                                     .toString();
        try {
            return this.connect(url);
        } catch (SQLException e) {
            throw new BackendException("Failed to access %s", e, jdbcUrl);
        }
    }

    /**
     * Connect DB with specified database, but won't auto reconnect
     */
    protected Connection openWithDB(int timeout) {
        String jdbcUrl = this.config.get(MysqlOptions.JDBC_URL);
        if (jdbcUrl.endsWith("/")) {
            jdbcUrl = String.format("%s%s", jdbcUrl, this.database());
        } else {
            jdbcUrl = String.format("%s/%s", jdbcUrl, this.database());
        }
        String url = new URIBuilder().setPath(jdbcUrl)
                                     .setParameter("socketTimeout",
                                                   String.valueOf(timeout))
                                     .toString();
        try {
            return this.connect(url);
        } catch (SQLException e) {
            throw new BackendException("Failed to access %s", jdbcUrl);
        }
    }

    /**
     * Connect DB with specified database
     */
    private Connection open(boolean autoReconnect) throws SQLException {
        String url = this.config.get(MysqlOptions.JDBC_URL);
        if (url.endsWith("/")) {
            url = String.format("%s%s", url, this.database());
        } else {
            url = String.format("%s/%s", url, this.database());
        }

        int maxTimes = this.config.get(MysqlOptions.JDBC_RECONNECT_MAX_TIMES);
        int interval = this.config.get(MysqlOptions.JDBC_RECONNECT_INTERVAL);
        String sslMode = this.config.get(MysqlOptions.JDBC_SSL_MODE);

        URIBuilder uriBuilder = this.newConnectionURIBuilder();
        uriBuilder.setPath(url)
                  .setParameter("useSSL", sslMode)
                  .setParameter("characterEncoding", "utf-8")
                  .setParameter("rewriteBatchedStatements", "true")
                  .setParameter("useServerPrepStmts", "false")
                  .setParameter("autoReconnect", String.valueOf(autoReconnect))
                  .setParameter("maxReconnects", String.valueOf(maxTimes))
                  .setParameter("initialTimeout", String.valueOf(interval));
        return this.connect(uriBuilder.toString());
    }

    protected URIBuilder newConnectionURIBuilder() {
        return new URIBuilder();
    }

    private Connection connect(String url) throws SQLException {
        String driverName = this.config.get(MysqlOptions.JDBC_DRIVER);
        String username = this.config.get(MysqlOptions.JDBC_USERNAME);
        String password = this.config.get(MysqlOptions.JDBC_PASSWORD);
        try {
            // Register JDBC driver
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            throw new BackendException("Invalid driver class '%s'",
                                       driverName);
        }
        return DriverManager.getConnection(url, username, password);
    }

    public class Session extends BackendSession {

        private Connection conn;
        private Map<String, PreparedStatement> statements;
        private int count;

        public Session() {
            this.conn = null;
            this.statements = new HashMap<>();
            this.count = 0;
        }

        public HugeConfig config() {
            return MysqlSessions.this.config();
        }

        @Override
        public void open() {
            try {
                this.doOpen();
            } catch (SQLException e) {
                throw new BackendException("Failed to open connection", e);
            }
        }

        public void tryOpen() {
            try {
                this.doOpen();
            } catch (SQLException ignored) {
                // Ignore
            }
        }

        public void doOpen() throws SQLException {
            if (this.conn != null && !this.conn.isClosed()) {
                return;
            }
            this.conn = MysqlSessions.this.open(true);
            this.opened = true;
        }

        @Override
        public void close() {
            assert this.closeable();
            if (this.conn == null) {
                return;
            }

            SQLException exception = null;
            for (PreparedStatement statement : this.statements.values()) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    exception = e;
                }
            }

            try {
                this.conn.close();
            } catch (SQLException e) {
                exception = e;
            }

            this.opened = false;
            this.conn = null;
            if (exception != null) {
                throw new BackendException("Failed to close connection",
                                           exception);
            }
        }

        @Override
        public boolean opened() {
            if (this.opened && this.conn == null) {
                // Reconnect if the connection is reset
                tryOpen();
            }
            return this.opened && this.conn != null;
        }

        @Override
        public boolean closed() {
            if (!this.opened || this.conn == null) {
                return true;
            }
            try {
                return this.conn.isClosed();
            } catch (SQLException ignored) {
                // Assume closed here
                return true;
            }
        }

        public void clear() {
            this.count = 0;
            SQLException exception = null;
            for (PreparedStatement statement : this.statements.values()) {
                try {
                    statement.clearBatch();
                } catch (SQLException e) {
                    exception = e;
                }
            }
            if (exception != null) {
                /*
                 * Will throw exception when the database connection error,
                 * we clear statements because clearBatch() failed
                 */
                this.statements = new HashMap<>();
            }
        }

        public void begin() throws SQLException {
            this.conn.setAutoCommit(false);
        }

        public void end() throws SQLException {
            this.conn.setAutoCommit(true);
        }

        public void endAndLog() {
            try {
                this.conn.setAutoCommit(true);
            } catch (SQLException e) {
                LOG.warn("Failed to set connection to auto-commit status", e);
            }
        }

        @Override
        public Integer commit() {
            int updated = 0;
            try {
                for (PreparedStatement statement : this.statements.values()) {
                    updated += IntStream.of(statement.executeBatch()).sum();
                }
                this.conn.commit();
                this.clear();
            } catch (SQLException e) {
                throw new BackendException("Failed to commit", e);
            }
            this.endAndLog();
            return updated;
        }

        @Override
        public void rollback() {
            this.clear();
            try {
                this.conn.rollback();
            } catch (SQLException e) {
                throw new BackendException("Failed to rollback", e);
            } finally {
                this.endAndLog();
            }
        }

        @Override
        public boolean hasChanges() {
            return this.count > 0;
        }

        @Override
        protected void reconnectIfNeeded() {
            try {
                this.execute("SELECT 1;");
            } catch (SQLException ignored) {
                // pass
            }
        }

        @Override
        protected void reset() {
            // NOTE: this method may be called by other threads
            this.conn = null;
        }

        public ResultSet select(String sql) throws SQLException {
            assert this.conn.getAutoCommit();
            return this.conn.createStatement().executeQuery(sql);
        }

        public boolean execute(String sql) throws SQLException {
            /*
             * commit() or rollback() failed to set connection to auto-commit
             * status in prior transaction. Manually set to auto-commit here.
             */
            if (this.conn.getAutoCommit()) {
                this.end();
            }
            return this.conn.createStatement().execute(sql);
        }

        public void add(PreparedStatement statement) {
            try {
                // Add a row to statement
                statement.addBatch();
                this.count++;
            } catch (SQLException e) {
                throw new BackendException("Failed to add statement '%s' " +
                                           "to batch", e, statement);
            }
        }

        public PreparedStatement prepareStatement(String sqlTemplate)
                                                  throws SQLException {
            PreparedStatement statement = this.statements.get(sqlTemplate);
            if (statement == null) {
                statement = this.conn.prepareStatement(sqlTemplate);
                this.statements.putIfAbsent(sqlTemplate, statement);
            }
            return statement;
        }
    }
}
