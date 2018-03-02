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

import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;

import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.baidu.hugegraph.backend.store.BackendTable;
import com.baidu.hugegraph.backend.store.mysql.MysqlStore;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.Log;

public abstract class PaloStore extends MysqlStore {

    private static final Logger LOG = Log.logger(PaloStore.class);

    public PaloStore(BackendStoreProvider provider,
                     String database, String name) {
        super(provider, database, name);
    }

    @Override
    protected PaloSessions newSessionPool(HugeConfig config, String database) {
        LOG.info("Open palo session pool for {}", this);
        return new PaloSessions(config, database, this.tableNames());
    }

    private List<String> tableNames() {
        return this.tables().values().stream().map(BackendTable::table)
                   .collect(Collectors.toList());
    }
}
