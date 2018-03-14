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

package com.baidu.hugegraph.backend.store.palo;

import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.store.mysql.MysqlSessions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.date.SafeDateFormat;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

public class PaloSessions extends MysqlSessions {

    private static final Logger LOG = Log.logger(PaloStore.class);

    private final AtomicInteger counter;
    private final Map<Integer, ReadWriteLock> locks;

    private final Timer timer;
    private final PaloLoadTask loadTask;

    public PaloSessions(HugeConfig config, String database, String store,
                        List<String> tableDirs) {
        super(config, database, store);
        this.counter = new AtomicInteger();
        this.locks = new ConcurrentHashMap<>();
        // Scan disk files and restore session information
        this.restoreSessionInfo(config, tableDirs);

        this.timer = new Timer();
        long interval = config.get(PaloOptions.PALO_POLL_INTERVAL);
        this.loadTask = new PaloLoadTask(tableDirs);
        this.timer.schedule(this.loadTask, 0, interval * 1000);
    }

    private void restoreSessionInfo(HugeConfig config, List<String> tableDirs) {
        Set<Integer> sessionIds = PaloFile.scanSessionIds(config, tableDirs);
        for (Integer sessionId : sessionIds) {
            if (!this.locks.containsKey(sessionId)) {
                // Create a read write lock for every session
                this.locks.put(sessionId, new ReentrantReadWriteLock());
            }
        }
        // Update counter value to avoid new session has duplicate id with old
        int maxSessionId = 0;
        for (int sessionId : sessionIds) {
            if (sessionId > maxSessionId) {
                maxSessionId = sessionId;
            }
        }
        this.counter.addAndGet(maxSessionId);
    }

    @Override
    protected String buildCreateDatabase(String database) {
        return String.format("CREATE DATABASE IF NOT EXISTS %s;", database);
    }

    @Override
    protected final synchronized Session newSession() {
        int id = this.counter.incrementAndGet();
        this.locks.put(id, new ReentrantReadWriteLock());
        return new Session(id);
    }

    @Override
    public final synchronized Session session() {
        return (Session) super.getOrNewSession();
    }

    @Override
    public void close() {
        if (this.loadTask != null) {
            this.loadTask.join();
        }
        if (this.timer != null) {
            this.timer.cancel();
        }
        super.close();
    }

    public final class Session extends MysqlSessions.Session {

        private final int id;
        /**
         * Stores the number of the file for each table that has been recorded
         * table1 -> session-part{n}
         */
        private final Map<String, Integer> parts;
        /**
         * Store data rows for each table
         * table -> [row-1, row-2, ...row-n]
         */
        private final Multimap<String, String> batch;

        public Session(int id) {
            super();
            this.id = id;
            this.parts = new HashMap<>();
            this.batch = LinkedListMultimap.create();
        }

        public void add(String table, String row) {
            this.batch.put(table, row);
            this.parts.putIfAbsent(table, 0);
        }

        @Override
        public Integer commit() {
            int updated = 0;
            if (!this.batch.isEmpty()) {
                updated += this.writeBatch();
            }
            updated += super.commit();
            this.clear();
            return updated;
        }

        @Override
        public void rollback() {
            super.rollback();
            this.clear();
        }

        @Override
        public void clear() {
            super.clear();
            this.batch.clear();
        }

        private int writeBatch() {
            int updated = 0;
            PaloSessions.this.locks.get(this.id).writeLock().lock();
            try {
                for (String table : this.batch.keySet()) {
                    PaloFile file = this.getOrCreate(table);
                    updated += file.writeLines(this.batch.get(table));
                }
            } finally {
                PaloSessions.this.locks.get(this.id).writeLock().unlock();
            }
            return updated;
        }

        private PaloFile getOrCreate(String table) {
            String tempDir = config().get(PaloOptions.PALO_TEMP_DIR);
            long limitSize = PaloFile.limitSize(config());

            // The table data path: 'palo-data/property_keys'
            String path = Paths.get(tempDir, table).toString();
            int part = this.parts.get(table);
            // The full file name: 'palo-data/property_keys/session1-part0'
            PaloFile file = new PaloFile(path, this.id, part);
            // Increase part number when file size exceed limit size
            if (file.length() >= limitSize) {
                this.parts.put(table, ++part);
                file = new PaloFile(path, this.id, part);
            }
            return file;
        }

        @SuppressWarnings("unused")
        private PaloLoadInfo getLoadInfoByLabel(String label) {
            String sql = String.format("SHOW LOAD WHERE LABEL = '%s'", label);
            ResultSet result;
            try {
                result = this.select(sql);
            } catch (SQLException e) {
                throw new BackendException("Failed to fetch load info " +
                                           "for label '%s'", e, label);
            }
            try {
                if (result.next()) {
                    return new PaloLoadInfo(result);
                }
                throw new BackendException("Non-exist load label '%s'", label);
            } catch (SQLException e) {
                throw new BackendException("Failed to fetch load info " +
                                           "for label '%s'", e, label);
            }
        }
    }

    public final class PaloLoadTask extends TimerTask {

        private static final String DF = "yyyy-MM-dd-HH-mm-ss";
        private final DateFormat DATE_FORMAT = new SafeDateFormat(DF);

        /**
         * There exist two running palo load task corresponds to two stores,
         * `SchemaStore -> SchemaLoadTask` and `GraphStore -> GraphLoadTask`,
         * the load task just handle with it's own subdirectory files,
         * like: `property_keys, vertex_labels ...` for SchemaLoadTask,
         * and: `vertices, edges ...` for GraphLoadTask
         * these subdirectory called validSubDirs.
         */
        private final List<String> tableDirs;
        /**
         *          session1-part1.txt  session2-part1.txt  ...
         * vertices 128m                125m                ...
         * edges    256m                250m                ...
         * ...      ...                 ...                 ...
         */
        private List<PaloFile> lastPaloFiles;
        private final PaloHttpClient client;

        public PaloLoadTask(List<String> tableDirs) {
            this.tableDirs = tableDirs;
            this.lastPaloFiles = null;
            this.client = new PaloHttpClient(config(), database());
        }

        /**
         * TODO: Need to be optimized
         */
        public void join() {
            Integer interval = config().get(PaloOptions.PALO_POLL_INTERVAL);
            while (this.lastPaloFiles != null &&
                   !this.lastPaloFiles.isEmpty()) {
                try {
                    TimeUnit.SECONDS.sleep(interval);
                } catch (InterruptedException e) {
                    throw new BackendException(e);
                }
            }
        }

        @Override
        public void run() {
            LOG.debug("The Load task:{} ready to run", PaloSessions.this);
            // Scan the directory to get all file size
            String path = config().get(PaloOptions.PALO_TEMP_DIR);
            List<PaloFile> paloFiles = PaloFile.scan(path, this.tableDirs);
            // Do nothing if there is no file at present
            if (paloFiles.isEmpty()) {
                return;
            }
            // Try to load one batch if last time and this time have some files
            if (this.lastPaloFiles != null) {
                this.tryLoadBatch(paloFiles);
            }
            // Update the last palo files
            this.lastPaloFiles = paloFiles;
        }

        private void tryLoadBatch(List<PaloFile> files) {
            PaloFile file = this.peekFile(files);
            // Load the first file when stopped inserting data
            this.loadThenDelete(file);
            files.remove(file);
        }

        private PaloFile peekFile(List<PaloFile> files) {
            assert !files.isEmpty();
            long limitSize = PaloFile.limitSize(config());
            // Load the file which exceed limit size in priority
            for (PaloFile file : files) {
                long fileSize = file.length();
                if (fileSize >= limitSize) {
                    return file;
                }
            }
            // Load the oldest file(files are sorted by updated time)
            return files.get(0);
        }

        private void loadThenDelete(PaloFile file) {
            // Parse session id from file name
            int sessionId = file.sessionId();
            LOG.info("Ready to load one batch from file: {}", file);
            // Get write lock because will delete file
            Lock lock = PaloSessions.this.locks.get(sessionId).writeLock();
            lock.lock();
            try {
                String table = file.table();;
                String data = file.readAsString();
                String label = this.formatLabel(table);
                this.client.bulkLoadAsync(table, data, label);
                // Force delete file
                file.forceDelete();
            } finally {
                lock.unlock();
            }
        }

        private String formatLabel(String table) {
            return table + "-" + this.DATE_FORMAT.format(new Date());
        }
    }
}
