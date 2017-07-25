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
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.baidu.hugegraph.backend.store;

import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.type.HugeType;

public interface BackendStore {

    // Database name
    public String name();

    // Get the parent provider
    public BackendStoreProvider provider();

    // Open/close database
    public void open(HugeConfig config);
    public void close();

    // Initialize/clear database
    public void init();
    public void clear();

    // Add/delete data
    public void mutate(BackendMutation mutation);

    // Query data
    public Iterable<BackendEntry> query(Query query);

    // Transaction
    public void beginTx();
    public void commitTx();
    public void rollbackTx();

    // Get metadata by key
    public Object metadata(HugeType type, String meta, Object[] args);
}
