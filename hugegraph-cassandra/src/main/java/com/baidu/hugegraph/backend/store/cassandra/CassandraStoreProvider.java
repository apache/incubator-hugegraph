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

import com.baidu.hugegraph.backend.store.AbstractBackendStoreProvider;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.cassandra.CassandraStore.CassandraGraphStore;
import com.baidu.hugegraph.backend.store.cassandra.CassandraStore.CassandraSchemaStore;

public class CassandraStoreProvider extends AbstractBackendStoreProvider {

    protected String keyspace() {
        return this.graph().toLowerCase();
    }

    @Override
    protected BackendStore newSchemaStore(String store) {
        return new CassandraSchemaStore(this, this.keyspace(), store);
    }

    @Override
    protected BackendStore newGraphStore(String store) {
        return new CassandraGraphStore(this, this.keyspace(), store);
    }

    @Override
    public String type() {
        return "cassandra";
    }

    @Override
    public String version() {
        /*
         * Versions history:
         * [1.0] HugeGraph-1328: supports backend table version checking
         * [1.1] HugeGraph-1322: add support for full-text search
         * [1.2] #296: support range sortKey feature
         * [1.3] #455: fix scylladb backend doesn't support label query in page
         * [1.4] #270 & #398: support shard-index and vertex + sortkey prefix,
         *                    also split range table to rangeInt, rangeFloat,
         *                    rangeLong and rangeDouble
         * [1.5] #633: support unique index
         * [1.6] #661 & #680: support bin serialization for cassandra
         * [1.7] #691: support aggregate property
         * [1.8] #746: support userdata for indexlabel
         * [1.9] #295: support ttl for edge label
         */
        return "1.9";
    }
}
