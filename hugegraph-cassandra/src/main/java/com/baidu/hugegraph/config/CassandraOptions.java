package com.baidu.hugegraph.config;

import static com.baidu.hugegraph.config.OptionChecker.disallowEmpty;
import static com.baidu.hugegraph.config.OptionChecker.rangeInt;

/**
 * Created by liningrui on 2017/5/25.
 */
public class CassandraOptions extends OptionHolder {

    private CassandraOptions() {
        super();
    }

    private static volatile CassandraOptions instance;

    public static CassandraOptions Instance() {
        if (instance == null) {
            synchronized (CassandraOptions.class) {
                if (instance == null) {
                    instance = new CassandraOptions();
                    instance.registerOptions();
                }
            }
        }
        return instance;
    }

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

}
