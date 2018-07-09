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

package com.baidu.hugegraph.plugin;

import com.baidu.hugegraph.analyzer.AnalyzerFactory;
import com.baidu.hugegraph.backend.serializer.SerializerFactory;
import com.baidu.hugegraph.backend.store.BackendProviderFactory;
import com.baidu.hugegraph.config.OptionSpace;

public interface HugeGraphPlugin {

    public String name();

    public void register();

    public String supportsMinVersion();

    public String supportsMaxVersion();

    public static void registerOptions(String name, String classPath) {
        OptionSpace.register(name, classPath);
    }

    public static void registerBackend(String name, String classPath) {
        BackendProviderFactory.register(name, classPath);
    }

    public static void registerSerializer(String name, String classPath) {
        SerializerFactory.register(name, classPath);
    }

    public static void registerAnalyzer(String name, String classPath) {
        AnalyzerFactory.register(name, classPath);
    }
}
