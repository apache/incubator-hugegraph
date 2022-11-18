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

package org.apache.hugegraph.dist;

import java.net.URL;
import java.util.List;
import java.util.ServiceLoader;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.backend.serializer.SerializerFactory;
import org.apache.hugegraph.backend.store.BackendProviderFactory;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.plugin.HugeGraphPlugin;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.VersionUtil;
import org.apache.hugegraph.version.CoreVersion;

public class RegisterUtil {

    private static final Logger LOG = Log.logger(RegisterUtil.class);

    static {
        OptionSpace.register("core", CoreOptions.instance());
        OptionSpace.register("dist", DistOptions.instance());
    }

    public static void registerBackends() {
        String confFile = "/backend.properties";
        URL input = RegisterUtil.class.getResource(confFile);
        E.checkState(input != null,
                     "Can't read file '%s' as stream", confFile);

        PropertiesConfiguration props = null;
        try {
            props = new Configurations().properties(input);
        } catch (ConfigurationException e) {
            throw new HugeException("Can't load config file: %s", e, confFile);
        }

        HugeConfig config = new HugeConfig(props);
        List<String> backends = config.get(DistOptions.BACKENDS);
        for (String backend : backends) {
            registerBackend(backend);
        }
    }

    private static void registerBackend(String backend) {
        switch (backend) {
            case "cassandra":
                registerCassandra();
                break;
            case "scylladb":
                registerScyllaDB();
                break;
            case "hbase":
                registerHBase();
                break;
            case "rocksdb":
                registerRocksDB();
                break;
            case "mysql":
                registerMysql();
                break;
            case "palo":
                registerPalo();
                break;
            case "postgresql":
                registerPostgresql();
                break;
            default:
                throw new HugeException("Unsupported backend type '%s'",
                                        backend);
        }
    }

    public static void registerCassandra() {
        // Register config
        OptionSpace.register("cassandra",
                "org.apache.hugegraph.backend.store.cassandra.CassandraOptions");
        // Register serializer
        SerializerFactory.register("cassandra",
                "org.apache.hugegraph.backend.store.cassandra.CassandraSerializer");
        // Register backend
        BackendProviderFactory.register("cassandra",
                "org.apache.hugegraph.backend.store.cassandra.CassandraStoreProvider");
    }

    public static void registerScyllaDB() {
        // Register config
        OptionSpace.register("scylladb",
                "org.apache.hugegraph.backend.store.cassandra.CassandraOptions");
        // Register serializer
        SerializerFactory.register("scylladb",
                "org.apache.hugegraph.backend.store.cassandra.CassandraSerializer");
        // Register backend
        BackendProviderFactory.register("scylladb",
                "org.apache.hugegraph.backend.store.scylladb.ScyllaDBStoreProvider");
    }

    public static void registerHBase() {
        // Register config
        OptionSpace.register("hbase",
                "org.apache.hugegraph.backend.store.hbase.HbaseOptions");
        // Register serializer
        SerializerFactory.register("hbase",
                "org.apache.hugegraph.backend.store.hbase.HbaseSerializer");
        // Register backend
        BackendProviderFactory.register("hbase",
                "org.apache.hugegraph.backend.store.hbase.HbaseStoreProvider");
    }

    public static void registerRocksDB() {
        // Register config
        OptionSpace.register("rocksdb",
                "org.apache.hugegraph.backend.store.rocksdb.RocksDBOptions");
        // Register backend
        BackendProviderFactory.register("rocksdb",
                "org.apache.hugegraph.backend.store.rocksdb.RocksDBStoreProvider");
        BackendProviderFactory.register("rocksdbsst",
                "org.apache.hugegraph.backend.store.rocksdbsst.RocksDBSstStoreProvider");
    }

    public static void registerMysql() {
        // Register config
        OptionSpace.register("mysql",
                "org.apache.hugegraph.backend.store.mysql.MysqlOptions");
        // Register serializer
        SerializerFactory.register("mysql",
                "org.apache.hugegraph.backend.store.mysql.MysqlSerializer");
        // Register backend
        BackendProviderFactory.register("mysql",
                "org.apache.hugegraph.backend.store.mysql.MysqlStoreProvider");
    }

    public static void registerPalo() {
        // Register config
        OptionSpace.register("palo",
                "org.apache.hugegraph.backend.store.palo.PaloOptions");
        // Register serializer
        SerializerFactory.register("palo",
                "org.apache.hugegraph.backend.store.palo.PaloSerializer");
        // Register backend
        BackendProviderFactory.register("palo",
                "org.apache.hugegraph.backend.store.palo.PaloStoreProvider");
    }

    public static void registerPostgresql() {
        // Register config
        OptionSpace.register("postgresql",
                "org.apache.hugegraph.backend.store.postgresql.PostgresqlOptions");
        // Register serializer
        SerializerFactory.register("postgresql",
                "org.apache.hugegraph.backend.store.postgresql.PostgresqlSerializer");
        // Register backend
        BackendProviderFactory.register("postgresql",
                "org.apache.hugegraph.backend.store.postgresql.PostgresqlStoreProvider");
    }

    public static void registerServer() {
        // Register ServerOptions (rest-server)
        OptionSpace.register("server", "org.apache.hugegraph.config.ServerOptions");
        // Register RpcOptions (rpc-server)
        OptionSpace.register("rpc", "org.apache.hugegraph.config.RpcOptions");
        // Register AuthOptions (auth-server)
        OptionSpace.register("auth", "org.apache.hugegraph.config.AuthOptions");
    }

    /**
     * Scan the jars in plugins directory and load them
     */
    public static void registerPlugins() {
        ServiceLoader<HugeGraphPlugin> plugins = ServiceLoader.load(
                                                 HugeGraphPlugin.class);
        for (HugeGraphPlugin plugin : plugins) {
            LOG.info("Loading plugin {}({})",
                     plugin.name(), plugin.getClass().getCanonicalName());
            String minVersion = plugin.supportsMinVersion();
            String maxVersion = plugin.supportsMaxVersion();

            if (!VersionUtil.match(CoreVersion.VERSION, minVersion,
                                   maxVersion)) {
                LOG.warn("Skip loading plugin '{}' due to the version range " +
                         "'[{}, {})' that it's supported doesn't cover " +
                         "current core version '{}'", plugin.name(),
                         minVersion, maxVersion, CoreVersion.VERSION.get());
                continue;
            }
            try {
                plugin.register();
                LOG.info("Loaded plugin '{}'", plugin.name());
            } catch (Exception e) {
                throw new HugeException("Failed to load plugin '%s'",
                                        plugin.name(), e);
            }
        }
    }
}
