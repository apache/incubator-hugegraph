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

import java.io.UncheckedIOException;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public class BackendStoreSystemInfo {

    private static final Logger LOG = Log.logger(HugeGraph.class);

    private static final String PK_BACKEND_INFO = Hidden.hide("backend_info");

    private final SchemaTransaction schemaTx;

    public BackendStoreSystemInfo(SchemaTransaction schemaTx) {
        this.schemaTx = schemaTx;
    }

    public void init() {
        if (this.exist()) {
            return;
        }
        // Set schema counter to reserve primitive system id
        this.schemaTx.setNextIdLowest(HugeType.SYS_SCHEMA,
                                      SchemaElement.MAX_PRIMITIVE_SYS_ID);

        // Use property key to store backend version
        HugeGraph graph = this.schemaTx.graph();
        String backendVersion = graph.backendVersion();
        PropertyKey backendInfo = graph.schema()
                                       .propertyKey(PK_BACKEND_INFO)
                                       .userdata("version", backendVersion)
                                       .build();
        this.schemaTx.addPropertyKey(backendInfo);
    }

    private Map<String, Object> info() {
        PropertyKey pkey = null;
        try {
            pkey = this.schemaTx.getPropertyKey(PK_BACKEND_INFO);
        } catch (BackendException | IllegalStateException |
                 UncheckedIOException ignored) {
            // pass
        }
        return pkey != null ? pkey.userdata() : null;
    }

    public boolean exist() {
        return this.info() != null;
    }

    public boolean checkVersion() {
        Map<String, Object> info = this.info();
        E.checkState(info != null, "The backend version info doesn't exist");
        // Backend has been initialized
        HugeGraph graph = this.schemaTx.graph();
        String driverVersion = graph.backendVersion();
        String backendVersion = (String) info.get("version");
        if (!driverVersion.equals(backendVersion)) {
            LOG.error("The backend driver version '{}' is inconsistent with " +
                      "the data version '{}' of backend store for graph '{}'",
                      driverVersion, backendVersion, graph.name());
            return false;
        }
        return true;
    }
}
