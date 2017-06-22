package com.baidu.hugegraph.config;

import static com.baidu.hugegraph.config.ConfigVerifier.disallowEmpty;
import static com.baidu.hugegraph.config.ConfigVerifier.rangeInt;

/**
 * Created by liningrui on 2017/5/25.
 */
public class ServerOptions extends OptionHolder {

    private ServerOptions() {
        super();
    }

    private static volatile ServerOptions instance;

    public static ServerOptions Instance() {
        if (instance == null) {
            synchronized (ServerOptions.class) {
                if (instance == null) {
                    instance = new ServerOptions();
                    instance.registerOptions();
                }
            }
        }
        return instance;
    }

    public static final ConfigOption<String> HUGE_SERVER_URL =
            new ConfigOption<>(
                    "hugeserver.url",
                    "http://127.0.0.1:8080",
                    true,
                    "The url for listening of hugeserver.",
                    disallowEmpty(String.class)
            );

    public static final ConfigOption<String> GREMLIN_SERVER_URL =
            new ConfigOption<>(
                    "gremlinserver.url",
                    "http://127.0.0.1:8182",
                    true,
                    "The url of gremlin server.",
                    disallowEmpty(String.class)
            );

    public static final ConfigOption<Integer> MAX_VERTICES_PER_BATCH =
            new ConfigOption<>(
                    "max_vertices_per_batch",
                    500,
                    true,
                    "The maximum number of vertices submitted per batch.",
                    rangeInt(100, 1000)
            );

    public static final ConfigOption<Integer> MAX_EDGES_PER_BATCH =
            new ConfigOption<>(
                    "max_edges_per_batch",
                    500,
                    true,
                    "The maximum number of edges submitted per batch.",
                    rangeInt(100, 1000)
            );
}
