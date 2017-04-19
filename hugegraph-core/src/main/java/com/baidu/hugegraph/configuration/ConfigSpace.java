package com.baidu.hugegraph.configuration;

import static com.baidu.hugegraph.configuration.ConfigVerifier.disallowEmpty;
import static com.baidu.hugegraph.configuration.ConfigVerifier.rangeInt;

import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * Created by liningrui on 2017/3/27.
 */
public class ConfigSpace {

    private static final Map<String, ConfigOption> configOptions = Maps.newHashMap();

    public static final ConfigOption<String> BACKEND = new ConfigOption<>(
            "backend",
            "memory",
            true,
            "The data store type.",
            disallowEmpty(String.class)
    );

    public static final ConfigOption<String> TABLE_SCHEMA = new ConfigOption<>(
            "table.schema",
            "huge_schema",
            true,
            "the table store graph schema info.",
            disallowEmpty(String.class)
    );

    public static final ConfigOption<String> TABLE_GRAPH = new ConfigOption<>(
            "table.graph",
            "huge_graph",
            true,
            "the table store graph vertex, edge and property info.",
            disallowEmpty(String.class)
    );

    public static final ConfigOption<String> CASSANDRA_HOST = new ConfigOption<>(
            "cassandra.host",
            "localhost",
            true,
            "The seeds hostname or ip address of cassandra cluster.",
            disallowEmpty(String.class)
    );

    public static final ConfigOption<Integer> CASSANDRA_PORT = new ConfigOption<>(
            "cassandra.port",
            9042,
            true,
            "The seeds port address of cassandra cluster.",
            rangeInt(1024, 10000)
    );

    public static final ConfigOption<String> CASSANDRA_KEYSPACE = new ConfigOption<String>(
            "cassandra.keyspace",
            "hugegraph",
            true,
            "keyspace name",
            disallowEmpty(String.class)
    );

    public static final ConfigOption<String> CASSANDRA_STRATEGY = new ConfigOption<String>(
            "cassandra.strategy",
            "hugegraph",
            true,
            "keyspace strategy",
            disallowEmpty(String.class)
    );

    public static final ConfigOption<Integer> CASSANDRA_REPLICATION = new ConfigOption<Integer>(
            "cassandra.replication",
            1,
            true,
            "replication factor",
            rangeInt(1, 100)
    );

    /**
     * 每个option只会被注册一次
     * @param element
     */
    public static void register(ConfigOption element) {
        Preconditions.checkNotNull(element);
        Preconditions.checkArgument(!configOptions.containsKey(element.name()),
                "A configuration element with the same name has already been added to this namespace: %s",
                element.name());
        configOptions.put(element.name(), element);
    }

    public static Boolean containKey(String key) {
        Preconditions.checkNotNull(key);
        return configOptions.containsKey(key);
    }

    public static ConfigOption get(String key) {
        Preconditions.checkNotNull(key);
        return configOptions.get(key);
    }
}
