package com.baidu.hugegraph.server;

import com.baidu.hugegraph.HugeException;

/**
 * Created by liningrui on 2017/5/10.
 */
public class HugeGraphServer {

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            throw new HugeException("HugeGraphServer can only accept one configuration file.");
        }

        RegisterUtil.registerComponent(args[0]);

        // start HugeGraphServer
        HugeServer.main(args);
    }

}
