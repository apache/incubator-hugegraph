/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.backend.store.jvector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendSession.AbstractBackendSession;
import org.apache.hugegraph.backend.store.BackendSessionPool;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

/**
 * Vector sessions for managing vector index operations
 */
public class VectorSessions extends BackendSessionPool {

    private static final Logger LOG = Log.logger(VectorSessions.class);

    private final String dataPath;
    private final HugeConfig config;
    private final Map<HugeType, VectorTable> tables;
    private boolean closed;

    public VectorSessions(String dataPath, HugeConfig config) {
        super(config, dataPath);
        this.dataPath = dataPath;
        this.config = config;
        this.tables = new ConcurrentHashMap<>();
        this.closed = false;
    }


    public boolean close() {
        this.closed = true;
        // Close all tables
        for (VectorTable table : this.tables.values()) {
            try {
                table.close();
            } catch (Exception e) {
                LOG.warn("Failed to close vector table: {}", e.getMessage());
            }
        }
        this.tables.clear();
        return false;
    }

    @Override
    public void open() throws Exception {

    }

    @Override
    protected boolean opened() {
        return false;
    }

    @Override
    public Session session() {
        return (Session) super.getOrNewSession();
    }

    @Override
    protected Session newSession() {
        return new Session();
    }

    @Override
    protected void doClose() {
        this.close();
    }

    /**
     * Vector session for handling vector index operations
     */
    public static class Session extends AbstractBackendSession {

        private final Map<HugeType, VectorTable> tables;
        private final Map<HugeType, List<BackendEntry>> pendingChanges;

        public Session() {
            this.tables = new ConcurrentHashMap<>();
            this.pendingChanges = new ConcurrentHashMap<>();
        }

        @Override
        public void open() {
            this.opened = true;
        }

        @Override
        public void close() {
            assert this.closeable();
            this.opened = false;
        }

        @Override
        public boolean closed() {
            return !this.opened;
        }

        @Override
        public Object commit() {
            int count = 0;
            for (Map.Entry<HugeType, List<BackendEntry>> entry : this.pendingChanges.entrySet()) {
                HugeType type = entry.getKey();
                List<BackendEntry> changes = entry.getValue();
                
                if (!changes.isEmpty()) {
                    VectorTable table = this.getOrCreateTable(type);
                    for (BackendEntry change : changes) {
                        table.insert(this, change);
                        count++;
                    }
                }
            }
            
            // Clear pending changes after commit
            this.pendingChanges.clear();
            return count;
        }

        @Override
        public void rollback() {
            // Clear all pending changes
            this.pendingChanges.clear();
        }

        @Override
        public boolean hasChanges() {
            return !this.pendingChanges.isEmpty();
        }

        @Override
        public void reset() {
            this.pendingChanges.clear();
        }

        // Vector-specific methods
        public void put(HugeType type, BackendEntry entry) {
            this.pendingChanges.computeIfAbsent(type, k -> new ArrayList<>()).add(entry);
        }

        public void delete(HugeType type, BackendEntry entry) {
            // For vector index, deletion is handled by removing from index
            // This would typically be implemented in the vector backend
            LOG.debug("Vector delete operation: {} -> {}", type, entry);
        }

        public VectorTable getOrCreateTable(HugeType type) {
            return this.tables.computeIfAbsent(type, t -> {
                VectorTable table = new VectorTable(t);
                try {
                    table.init(this);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to initialize vector table", e);
                }
                return table;
            });
        }
    }

}
