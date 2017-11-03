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
     * Api Version change log
     *
     * 0.3.0: Firstly add version check.
     *
     * 0.4.0:
     * 1) Change edge-link to source label and target label
     * 2) Change `create` and `append` method of schema api to adapt the
     *    modification of the core module.
     */

    // The second parameter of Version.of() is for IDE running without JAR
    public static final Version VERSION = Version.of(ApiVersion.class, "0.10");

    public static final void check() {
        // Check version of hugegraph-core
        VersionUtil.check(CoreVersion.VERSION, "0.3", "0.4", CoreVersion.NAME);
    }
}
