package com.baidu.hugegraph.config;

import static com.baidu.hugegraph.config.ConfigVerifier.disallowEmpty;

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
            synchronized(ServerOptions.class) {
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

}
