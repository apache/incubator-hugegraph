package com.baidu.hugegraph.config;

import static com.baidu.hugegraph.config.ConfigVerifier.disallowEmpty;

/**
 * Created by liningrui on 2017/5/25.
 */
public class CoreOptions extends OptionHolder {

    private CoreOptions() {
        super();
    }

    private static volatile CoreOptions instance;

    public static CoreOptions Instance() {
        if (instance == null) {
            synchronized (CoreOptions.class) {
                if (instance == null) {
                    instance = new CoreOptions();
                    instance.registerOptions();
                }
            }
        }
        return instance;
    }

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

    public static final ConfigOption<String> DEFAULT_VERTEX_LABEL = new ConfigOption<>(
            "vertex.default_label",
            "v",
            true,
            "The default vertex label.",
            disallowEmpty(String.class)
    );

    public static final ConfigOption<String> GRAPHS =
            new ConfigOption<>(
                    "graphs",
                    "hugegraph:conf/hugegraph.properties",
                    true,
                    "The map of graphs' name and config file.",
                    disallowEmpty(String.class)
            );
}
