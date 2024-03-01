/*
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

package org.apache.hugegraph.backend.store.hstore;

import java.util.List;
import java.util.Map;

import org.apache.hugegraph.backend.store.BackendMetrics;
import org.apache.hugegraph.util.InsertionOrderUtil;

public class HstoreMetrics implements BackendMetrics {

    private final List<HstoreSessions> dbs;
    private final HstoreSessions.Session session;

    public HstoreMetrics(List<HstoreSessions> dbs,
                         HstoreSessions.Session session) {
        this.dbs = dbs;
        this.session = session;
    }

    @Override
    public Map<String, Object> metrics() {
        Map<String, Object> results = InsertionOrderUtil.newMap();
        // TODO(metrics): fetch more metrics from PD
        results.put(NODES, session.getActiveStoreSize());
        return results;
    }
}
