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

package com.baidu.hugegraph.version;

import com.baidu.hugegraph.util.VersionUtil;
import com.baidu.hugegraph.util.VersionUtil.Version;

public final class ApiVersion {

    public static final String NAME = "hugegraph-api";

    /**
     * API Version change log
     *
     * version 0.2:
     * [0.2] HugeGraph-527: First add the version to the hugegraph module
     * [0.3] HugeGraph-525: Add versions check of components and api
     * [0.4] HugeGraph-162: Add schema builder to seperate client and
     *       inner interface.
     * [0.5] HugeGraph-498: Support three kind of id strategy
     *
     * version 0.3:
     *
     * [0.6] HugeGraph-614: Add update api of VL/EL to support append and
     *       eliminate action
     * [0.7] HugeGraph-245: Add nullable-props for vertex label and edge label
     * [0.8] HugeGraph-396: Continue to improve variables implementation
     * [0.9] HugeGraph-894: Add vertex/edge update api to add property and
     *       remove property
     * [0.10] HugeGraph-919: Add condition query for vertex/edge list API
     *
     * version 0.4:
     * [0.11] HugeGraph-938: Remove useless indexnames field in VL/EL API
     * [0.12] HugeGraph-589: Add schema id for all schema element
     * [0.13] HugeGraph-956: Support customize string/number id strategy
     *
     * version 0.5:
     * [0.14] HugeGraph-1085: Add enable_label_index to VL/EL
     * [0.15] HugeGraph-1105: Support paging for large amounts of records
     * [0.16] HugeGraph-944: Support rest shortest path, k-out, k-neighbor
     * [0.17] HugeGraph-944: Support rest shortest path, k-out, k-neighbor
     * [0.18] HugeGraph-81: Change argument "checkVertex" to "check_vertex"
     *
     * version 0.6:
     * [0.19] HugeGraph-1195: Support eliminate userdata on schema
     * [0.20] HugeGraph-1210: Add paths api to find paths between two nodes
     * [0.21] HugeGraph-1197: Expose scan api for hugegraph-spark
     * [0.22] HugeGraph-1162: Support authentication and permission control
     * [0.23] HugeGraph-1176: Support degree and capacity for traverse api
     * [0.24] HugeGraph-1261: Add param offset for vertex/edge list API
     * [0.25] HugeGraph-1272: Support set/clear restore status of graph
     * [0.26] HugeGraph-1273: Add some monitoring counters to integrate with
     *        gremlin's monitoring framework
     */

    // The second parameter of Version.of() is for IDE running without JAR
    public static final Version VERSION = Version.of(ApiVersion.class, "0.26");

    public static final void check() {
        // Check version of hugegraph-core. Firstly do check from version 0.3
        VersionUtil.check(CoreVersion.VERSION, "0.7", "0.8", CoreVersion.NAME);
    }
}
