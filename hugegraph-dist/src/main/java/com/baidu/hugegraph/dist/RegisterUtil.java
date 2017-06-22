package com.baidu.hugegraph.dist;

import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.tree.ConfigurationNode;
import org.apache.tinkerpop.gremlin.util.config.YamlConfiguration;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.backend.serializer.SerializerFactory;
import com.baidu.hugegraph.backend.store.BackendProviderFactory;
import com.baidu.hugegraph.config.CassandraOptions;
import com.baidu.hugegraph.config.ConfigSpace;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.exception.ConfigException;
import com.baidu.hugegraph.util.E;

/**
 * Created by liningrui on 2017/5/12.
 */
public class RegisterUtil {

    static {
        ConfigSpace.register(CoreOptions.Instance());
    }

    public static void registerBackends() throws ConfigurationException {
        String confFile = "/backend.properties";
        InputStream is = RegisterUtil.class.getClass().getResourceAsStream(confFile);
        E.checkNotNull(is, "Can't read file '%s' as stream", confFile);

        HugeConfig config = new HugeConfig(is);
        List<Object> backends = config.getList(CoreOptions.BACKENDS.name());
        for (Object backend : backends) {
            registerBackend((String) backend);
        }
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
                throw new HugeException("Unknown backend type '%s'", backend);
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
