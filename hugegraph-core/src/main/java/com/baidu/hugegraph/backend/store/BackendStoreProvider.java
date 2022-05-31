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

package com.baidu.hugegraph.backend.store;

import com.alipay.remoting.rpc.RpcServer;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.event.EventListener;

public interface BackendStoreProvider {

    String SCHEMA_STORE = "m";
    String GRAPH_STORE = "g";
    String SYSTEM_STORE = "s";

    // Backend store type
    String type();

    // Backend store version
    String storedVersion();

    // Current backend store driver version
    String driverVersion();

    // Graph name (that's database name)
    String graph();

    BackendStore loadSystemStore(HugeConfig config);

    BackendStore loadSchemaStore(HugeConfig config);

    BackendStore loadGraphStore(HugeConfig config);

    void open(String name);

    void waitReady(RpcServer rpcServer);

    void close();

    void init();

    void clear();

    boolean initialized();

    void truncate();

    void createSnapshot();

    void resumeSnapshot();

    void listen(EventListener listener);

    void unlisten(EventListener listener);

    EventHub storeEventHub();

    void onCloneConfig(HugeConfig config, String newGraph);

    void onDeleteConfig(HugeConfig config);
}
