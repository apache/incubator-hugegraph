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

import org.apache.hugegraph.backend.store.AbstractBackendStoreProvider;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

/**
 * Vector backend store provider for vector index support
 * This provider delegates schema and system operations to the underlying provider,
 * but handles vector index operations with vector backend
 */
public class VectorBackendStoreProvider extends AbstractBackendStoreProvider {

    private static final Logger LOG = Log.logger(VectorBackendStoreProvider.class);

    private final BackendStoreProvider underlyingProvider;
    private VectorBackendStore vectorStore;

    public VectorBackendStoreProvider(BackendStoreProvider underlyingProvider) {
        this.underlyingProvider = underlyingProvider;
    }

    @Override
    protected BackendStore newSchemaStore(HugeConfig config, String store) {
        // Delegate schema operations to underlying provider
        return this.underlyingProvider.loadSchemaStore(config);
    }

    @Override
    protected BackendStore newGraphStore(HugeConfig config, String store) {
        // Delegate graph operations to underlying provider
        // Vector backend only handles vector index operations
        return this.underlyingProvider.loadGraphStore(config);
    }

    @Override
    protected BackendStore newSystemStore(HugeConfig config, String store) {
        // Delegate system operations to underlying provider
        return this.underlyingProvider.loadSystemStore(config);
    }

    @Override
    public String type() {
        return "vector";
    }

    @Override
    public String driverVersion() {
        return "1.0.0";
    }

    @Override
    public void close() {
        this.underlyingProvider.close();
        super.close();
    }

    @Override
    public void onCloneConfig(HugeConfig config, String newGraph) {
        this.underlyingProvider.onCloneConfig(config, newGraph);
        super.onCloneConfig(config, newGraph);
    }

    @Override
    public void onDeleteConfig(HugeConfig config) {
        this.underlyingProvider.onDeleteConfig(config);
        super.onDeleteConfig(config);
    }

    protected String database() {
        return this.graph().toLowerCase();
    }
    /**
     * Get the underlying provider
     */
    public BackendStoreProvider underlyingProvider() {
        return this.underlyingProvider;
    }

    /**
     * Get the vector store
     */
    public VectorBackendStore vectorStore() {
        return this.vectorStore;
    }
}
