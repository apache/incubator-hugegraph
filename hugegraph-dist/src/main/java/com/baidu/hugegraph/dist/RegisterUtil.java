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

package com.baidu.hugegraph.dist;

import java.io.InputStream;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.backend.serializer.SerializerFactory;
import com.baidu.hugegraph.backend.store.BackendProviderFactory;
import com.baidu.hugegraph.config.CassandraOptions;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.OptionSpace;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.util.E;

public class RegisterUtil {

    static {
        OptionSpace.register(CoreOptions.Instance());
    }

    public static void registerBackends() throws ConfigurationException {
        String confFile = "/backend.properties";
        InputStream is = RegisterUtil.class.getClass()
                         .getResourceAsStream(confFile);
        E.checkState(is != null, "Can't read file '%s' as stream", confFile);

        HugeConfig config = new HugeConfig(is);
        List<Object> backends = config.getList(CoreOptions.BACKENDS.name());
        for (Object backend : backends) {
            registerBackend((String) backend);
        }
    }

    private static void registerBackend(String backend) {
        switch (backend) {
            case "cassandra":
                registerCassandra();
                break;
            case "hbase":
                registerHBase();
                break;
            default:
                throw new HugeException("Unsupported backend type '%s'", backend);
        }
    }

    public static void registerCassandra() {
        // register config
        OptionSpace.register(CassandraOptions.Instance());
        // register serializer
        SerializerFactory.register("cassandra",
                "com.baidu.hugegraph.backend.store.cassandra.CassandraSerializer");
        // register backend
        BackendProviderFactory.register("cassandra",
                "com.baidu.hugegraph.backend.store.cassandra.CassandraStoreProvider");
    }

    public static void registerHBase() {

    }

    public static void registerServer() {
        OptionSpace.register(ServerOptions.Instance());
    }
}
