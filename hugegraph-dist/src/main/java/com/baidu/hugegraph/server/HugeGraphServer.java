package com.baidu.hugegraph.server;

import static com.baidu.hugegraph.configuration.ConfigSpace.BACKEND;
import static com.baidu.hugegraph.configuration.ConfigSpace.SERIALIZER;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.tree.ConfigurationNode;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.util.config.YamlConfiguration;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.backend.serializer.SerializerFactory;
import com.baidu.hugegraph.backend.store.BackendProviderFactory;
import com.baidu.hugegraph.configuration.HugeConfiguration;

/**
 * Created by liningrui on 2017/5/10.
 */
public class HugeGraphServer {

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            throw new HugeException("HugeGraphServer can only accept one configuration file.");
        }

        registerComponent(args);

        // start GremlinServer
        GremlinServer.main(args);
    }

    private static void registerComponent(String[] args) throws ConfigurationException {
        YamlConfiguration yamlConfig = new YamlConfiguration();
        yamlConfig.load(args[0]);

        List<ConfigurationNode> graphs = yamlConfig.getRootNode()
                .getChildren("graphs").get(0).getChildren();

        Set<String> backends = new HashSet<>();
        Set<String> serializers = new HashSet<>();

        for (int i = 0; i < graphs.size(); i++) {
            System.out.println(graphs.get(i).getValue());
            String propConfFile = graphs.get(i).getValue().toString();

            // get graph property file path
            HugeConfiguration configuration = new HugeConfiguration(propConfFile);
            backends.add(configuration.get(BACKEND).toLowerCase());
            serializers.add(configuration.get(SERIALIZER).toLowerCase());
        }

        backends.forEach(backend -> registerBackendProvider(backend));
        serializers.forEach(serializer -> registerSerializer(serializer));
    }

    private static void registerSerializer(String serializer) {
        switch (serializer) {
            case "cassandra":
                // register serializer
                SerializerFactory.register("cassandra",
                        "com.baidu.hugegraph.backend.store.cassandra.CassandraSerializer");
                break;
            default:
                break;
        }

    }

    private static void registerBackendProvider(String backend) {
        switch (backend) {
            case "cassandra":
                // register backend provider
                BackendProviderFactory.register("cassandra",
                        "com.baidu.hugegraph.backend.store.cassandra.CassandraStoreProvider");
                break;
            default:
                break;
        }
    }

}
