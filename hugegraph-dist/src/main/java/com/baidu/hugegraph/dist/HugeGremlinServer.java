package com.baidu.hugegraph.dist;

import org.apache.tinkerpop.gremlin.server.GremlinServer;

import com.baidu.hugegraph.HugeException;

/**
 * Created by liningrui on 2017/5/10.
 */
public class HugeGremlinServer {

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            throw new HugeException("HugeGremlinServer can only accept one "
                    + "configuration file.");
        }

        RegisterUtil.registerCore();
        RegisterUtil.registerBackends(args[0]);

        // start GremlinServer
        GremlinServer.main(args);
    }

}
