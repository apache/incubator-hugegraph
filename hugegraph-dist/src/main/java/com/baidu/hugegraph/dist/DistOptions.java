package com.baidu.hugegraph.dist;

import static com.baidu.hugegraph.config.OptionChecker.disallowEmpty;

import com.baidu.hugegraph.config.ConfigListOption;
import com.baidu.hugegraph.config.OptionHolder;
import com.baidu.hugegraph.config.ServerOptions;

public class DistOptions extends OptionHolder {

    private DistOptions() {
        super();
    }

    private static volatile DistOptions instance;

    public static DistOptions instance() {
        if (instance == null) {
            synchronized (ServerOptions.class) {
                if (instance == null) {
                    instance = new DistOptions();
                    instance.registerOptions();
                }
            }
        }
        return instance;
    }

    public static final ConfigListOption<String> BACKENDS =
            new ConfigListOption<>(
                    "backends",
                    "The all data store type.",
                    disallowEmpty(),
                    "memory"
            );
}
