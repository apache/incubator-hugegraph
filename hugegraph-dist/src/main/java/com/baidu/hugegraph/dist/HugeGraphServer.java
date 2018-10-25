/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.dist;

import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.util.Log;

public class HugeGraphServer {

    private static final Logger LOG = Log.logger(HugeGraphServer.class);

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("HugeGraphServer stopped");
        }));
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            String msg = "HugeGraphServer can only accept two config files";
            LOG.error(msg);
            throw new HugeException(msg);
        }

        HugeRestServer.register();

        GremlinServer gremlinServer = null;
        try {
            // Start GremlinServer
            gremlinServer = HugeGremlinServer.start(args[0]);
        } catch (Exception e) {
            LOG.error("HugeGremlinServer start error: ", e);
            HugeGraph.shutdown(30L);
            throw e;
        }

        try {
            // Start HugeRestServer
            HugeRestServer.start(args[1]);
        } catch (Exception e) {
            LOG.error("HugeRestServer start error: ", e);
            gremlinServer.stop();
            HugeGraph.shutdown(30L);
            throw e;
        }
    }
}
