package com.baidu.hugegraph.dist;

import java.io.InputStream;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.backend.serializer.SerializerFactory;
import com.baidu.hugegraph.backend.store.BackendProviderFactory;
import com.baidu.hugegraph.config.CassandraOptions;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.OptionSpace;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.util.E;

/**
 * Created by liningrui on 2017/5/12.
 */
public class RegisterUtil {

    static {
        OptionSpace.register(CoreOptions.Instance());
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
        OptionSpace.register(CassandraOptions.Instance());
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
        OptionSpace.register(ServerOptions.Instance());
    }
}
