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

package com.baidu.hugegraph.backend.store.hstore;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.store.AbstractBackendStoreProvider;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.hstore.HstoreStore.HstoreGraphStore;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.util.Events;

public class HstoreProvider extends AbstractBackendStoreProvider {

    protected String namespace() {
        return this.graph();
    }

    @Override
    protected synchronized BackendStore newSchemaStore(String store) {
        return new HstoreStore.HstoreSchemaStore(this, this.namespace(),
                                                 store);
    }

    @Override
    protected BackendStore newGraphStore(String store) {
        return new HstoreGraphStore(this, this.namespace(), store);
    }

    @Override
    public void truncateGraph(HugeGraph graph) {
        this.checkOpened();
        String g = graph.option(CoreOptions.STORE_GRAPH);
        BackendStore store = this.stores.get(g);
        store.truncate();
        this.notifyAndWaitEvent(Events.STORE_TRUNCATE);
        LOG.debug("Graph '{}' store has been truncated", graph.name());
    }

    @Override
    public String type() {
        return "hstore";
    }

    @Override
    public String version() {
        /*
         * Versions history:
         * [1.0] HugeGraph-1328: supports hstore
         */
        return "1.11";
    }
}
