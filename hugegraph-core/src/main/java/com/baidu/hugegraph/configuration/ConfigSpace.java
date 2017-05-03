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

    private static final Map<String, ConfigOption<?>> configOptions = Maps.newHashMap();

    public static final ConfigOption<String> BACKEND = new ConfigOption<>(
            "backend",
            "memory",
            true,
            "The data store type.",
            disallowEmpty(String.class)
    );

    public static final ConfigOption<String> STORE = new ConfigOption<>(
            "store",
            "hugegraph",
            true,
            "The database name like Cassandra Keyspace.",
            disallowEmpty(String.class)
    );

    public static final ConfigOption<String> STORE_SCHEMA = new ConfigOption<>(
            "store.schema",
            "huge_schema",
            true,
            "The schema table name, which store meta data.",
            disallowEmpty(String.class)
    );

    public static final ConfigOption<String> STORE_GRAPH = new ConfigOption<>(
            "store.graph",
            "huge_graph",
            true,
            "The graph table name, which store vertex, edge and property.",
            disallowEmpty(String.class)
    );

    public static final ConfigOption<String> STORE_INDEX = new ConfigOption<>(
            "store.index",
            "huge_index",
            true,
            "The index table name, which store index data of vertex, edge.",
            disallowEmpty(String.class)
    );

    public static final ConfigOption<String> SERIALIZER = new ConfigOption<>(
            "serializer",
            "text",
            true,
            "The serializer for backend store, like: text/binary/cassandra",
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

    public static final ConfigOption<String> CASSANDRA_STRATEGY = new ConfigOption<String>(
            "cassandra.keyspace.strategy",
            "SimpleStrategy",
            true,
            "The keyspace strategy",
            disallowEmpty(String.class)
    );

    public static final ConfigOption<Integer> CASSANDRA_REPLICATION = new ConfigOption<Integer>(
            "cassandra.keyspace.replication",
            3,
            true,
            "The keyspace replication factor",
            rangeInt(1, 100)
    );

    public static void register(ConfigOption<?> element) {
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

    public static ConfigOption<?> get(String key) {
        Preconditions.checkNotNull(key);
        return configOptions.get(key);
    }
}
