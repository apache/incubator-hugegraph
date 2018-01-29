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

package com.baidu.hugegraph.example;

import java.io.File;

import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.dist.RegisterUtil;
import com.baidu.hugegraph.perf.PerfUtil;

public class ExampleUtil {

    private static boolean registered = false;

    public static void registerPlugins() {
        if (registered) {
            return;
        }
        registered = true;

        RegisterUtil.registerCassandra();
        RegisterUtil.registerScyllaDB();
        RegisterUtil.registerHBase();
        RegisterUtil.registerRocksDB();
        RegisterUtil.registerMysql();
    }

    public static HugeGraph loadGraph() {
        return loadGraph(true, false);
    }

    public static HugeGraph loadGraph(boolean needClear, boolean needProfile) {
        if (needProfile) {
            profile();
        }

        registerPlugins();

        String conf = "hugegraph.properties";
        try {
            String path = ExampleUtil.class.getClassLoader()
                                     .getResource(conf).getPath();
            File file = new File(path);
            if (file.exists() && file.isFile()) {
                conf = path;
            }
        } catch (Exception ignored) {
        }

        HugeGraph graph = HugeFactory.open(conf);

        if (needClear) {
            graph.clearBackend();
        }
        graph.initBackend();

        return graph;
    }

    public static void profile() {
        try {
            PerfUtil.instance().profilePackage("com.baidu.hugegraph");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
