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

        RegisterUtil.registerComponent(args[0]);

        // start GremlinServer
        GremlinServer.main(args);
    }

}
