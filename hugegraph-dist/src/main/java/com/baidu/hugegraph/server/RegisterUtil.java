package com.baidu.hugegraph.server;

import static com.baidu.hugegraph.configuration.ConfigSpace.BACKEND;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.tree.ConfigurationNode;
import org.apache.tinkerpop.gremlin.util.config.YamlConfiguration;

import com.baidu.hugegraph.backend.serializer.SerializerFactory;
import com.baidu.hugegraph.backend.store.BackendProviderFactory;
import com.baidu.hugegraph.configuration.HugeConfiguration;

/**
 * Created by liningrui on 2017/5/12.
 */
public class RegisterUtil {

    private static Set<String> backends = new HashSet<>();

    public static void registerComponent(String confFile) throws ConfigurationException {

        loadConfiguration(confFile);

        registerBackend();
    }

    private static void loadConfiguration(String confFile) throws ConfigurationException {
        YamlConfiguration yamlConfig = new YamlConfiguration();
        yamlConfig.load(confFile);

        List<ConfigurationNode> graphs = yamlConfig.getRootNode()
                .getChildren("graphs").get(0).getChildren();

        for (int i = 0; i < graphs.size(); i++) {
            String propConfFile = graphs.get(i).getValue().toString();

            // get graph property file path
            HugeConfiguration configuration = new HugeConfiguration(propConfFile);
            backends.add(configuration.get(BACKEND).toLowerCase());
        }
    }

    private static void registerBackend() {
        for (String backend : backends) {
            switch (backend) {
                case "cassandra":
                    registerCassandra();
                    break;
                case "hbase":
                    registerHBase();
                    break;
                default:
                    break;
            }
        }
    }

    public static void registerCassandra() {
        SerializerFactory.register("cassandra",
                "com.baidu.hugegraph.backend.store.cassandra.CassandraSerializer");
        BackendProviderFactory.register("cassandra",
                "com.baidu.hugegraph.backend.store.cassandra.CassandraStoreProvider");
    }

    public static void registerHBase() {

    }
}
