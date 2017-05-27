package com.baidu.hugegraph.dist;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.tree.ConfigurationNode;
import org.apache.tinkerpop.gremlin.util.config.YamlConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.backend.serializer.SerializerFactory;
import com.baidu.hugegraph.backend.store.BackendProviderFactory;
import com.baidu.hugegraph.config.CassandraOptions;
import com.baidu.hugegraph.config.ConfigSpace;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.exception.ConfigException;

/**
 * Created by liningrui on 2017/5/12.
 */
public class RegisterUtil {

    private static final Logger logger = LoggerFactory.getLogger(RegisterUtil.class);

    public static void registerCore() {
        ConfigSpace.register(CoreOptions.Instance());
    }

    public static void registerBackends(String confFile)
            throws ConfigurationException {

        Set<String> backends = null;
        if (confFile.endsWith(".yaml")) {
            YamlConfiguration config = new YamlConfiguration();
            config.load(confFile);
            backends = parseBackends(config);
        } else if (confFile.endsWith(".properties")) {
            HugeConfig config = new HugeConfig(confFile);
            backends = parseBackends(config);
        } else {
            throw new ConfigException(String.format("Configuration files in " +
                    "this format are not supported: %s", confFile));
        }
        for (String backend : backends) {
            registerBackend(backend);
        }
    }

    private static Set<String> parseBackends(YamlConfiguration config)
            throws ConfigurationException {
        List<ConfigurationNode> graphs = config.getRootNode()
                .getChildren(CoreOptions.GRAPHS.name()).get(0).getChildren();

        Set<String> backends = new HashSet<>();
        for (ConfigurationNode graph : graphs) {
            String propConfFile = graph.getValue().toString();
            // get graph property file path
            HugeConfig configuration = new HugeConfig(propConfFile);
            backends.add(configuration.get(CoreOptions.BACKEND).toLowerCase());
        }
        return backends;
    }

    private static Set<String> parseBackends(HugeConfig config)
            throws ConfigurationException {
        List<Object> graphs = config.getList(CoreOptions.GRAPHS.name());

        Set<String> backends = new HashSet<>();
        for (Object graph : graphs) {
            String[] graphPair = graph.toString().split(":");
            assert graphPair.length == 2;
            String propConfFile = graphPair[1];
            // get graph property file path
            HugeConfig configuration = new HugeConfig(propConfFile);
            backends.add(configuration.get(CoreOptions.BACKEND).toLowerCase());
        }
        return backends;
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
                break;
        }
    }

    public static void registerCassandra() {
        // register config
        ConfigSpace.register(CassandraOptions.Instance());
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
        ConfigSpace.register(ServerOptions.Instance());
    }
}
