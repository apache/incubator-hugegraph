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

import java.util.Iterator;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.store.AbstractBackendStoreProvider;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.mysql.MysqlStore.MysqlGraphStore;
import com.baidu.hugegraph.backend.store.mysql.MysqlStore.MysqlSchemaStore;
import com.baidu.hugegraph.util.Events;

public class MysqlStoreProvider extends AbstractBackendStoreProvider {

    protected String database() {
        return this.graph().toLowerCase();
    }

    @Override
    protected BackendStore newSchemaStore(String store) {
        return new MysqlSchemaStore(this, this.database(), store);
    }

    @Override
    protected BackendStore newGraphStore(String store) {
        return new MysqlGraphStore(this, this.database(), store);
    }

    @Override
    public void clear() throws BackendException {
        this.checkOpened();
        /*
         * We should drop database once only with stores(schema/graph/system),
         * otherwise it will easily lead to blocking when drop tables
         */
        Iterator<BackendStore> iter = this.stores.values().iterator();
        if (iter.hasNext()) {
            iter.next().clear();
        }
        this.notifyAndWaitEvent(Events.STORE_CLEAR);
    }

    @Override
    public String type() {
        return "mysql";
    }

    @Override
    public String version() {
        /*
         * Versions history:
         * [1.0] HugeGraph-1328: supports backend table version checking
         * [1.1] HugeGraph-1322: add support for full-text search
         * [1.2] #296: support range sortKey feature
         * [1.3] #270 & #398: support shard-index and vertex + sortkey prefix,
         *                    also split range table to rangeInt, rangeFloat,
         *                    rangeLong and rangeDouble
         * [1.4] #633: support unique index
         * [1.5] #661: reduce the storage of vertex/edge id
         * [1.6] #691: support aggregate property
         */
        return "1.6";
    }
}
