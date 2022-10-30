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

import java.util.Map;

public interface BackendMetrics {

    String BACKEND = "backend";

    String NODES = "nodes";
    String CLUSTER_ID = "cluster_id";
    String SERVERS = "servers";
    String SERVER_LOCAL = "local";
    String SERVER_CLUSTER = "cluster";

    // Memory related metrics
    String MEM_USED = "mem_used";
    String MEM_COMMITTED = "mem_committed";
    String MEM_MAX = "mem_max";
    String MEM_UNIT = "mem_unit";

    // Data load related metrics
    String DISK_USAGE = "disk_usage";
    String DISK_UNIT = "disk_unit";

    String READABLE = "_readable";

    String EXCEPTION = "exception";

    Map<String, Object> metrics();
}
