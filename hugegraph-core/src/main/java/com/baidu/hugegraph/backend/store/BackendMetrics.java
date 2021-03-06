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

    public String BACKEND = "backend";

    public String NODES = "nodes";
    public String CLUSTER_ID = "cluster_id";
    public String SERVERS = "servers";
    public String SERVER_LOCAL = "local";
    public String SERVER_CLUSTER = "cluster";

    // Memory related metrics
    public String MEM_USED = "mem_used";
    public String MEM_COMMITED = "mem_commited";
    public String MEM_MAX = "mem_max";
    public String MEM_UNIT = "mem_unit";

    // Data load related metrics
    public String DISK_USAGE = "disk_usage";
    public String DISK_UNIT = "disk_unit";

    public String READABLE = "_readable";

    public String EXCEPTION = "exception";

    public Map<String, Object> metrics();
}
