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

import com.baidu.hugegraph.perf.PerfUtil;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.util.Log;

public class HugeGraphServer {

    private static final Logger LOG = Log.logger(HugeGraphServer.class);

    private final GremlinServer gremlinServer;
    private final RestServer restServer;

    public HugeGraphServer(String gremlinServerConf, String restServerConf)
                           throws Exception {
        // Only switch on security manager after HugeGremlinServer started
        SecurityManager securityManager = System.getSecurityManager();
        System.setSecurityManager(null);
        try {
            // Start GremlinServer
            this.gremlinServer = HugeGremlinServer.start(gremlinServerConf);
        } catch (Throwable e) {
            LOG.error("HugeGremlinServer start error: ", e);
            HugeFactory.shutdown(30L);
            throw e;
        } finally {
            System.setSecurityManager(securityManager);
        }

        try {
            // Start HugeRestServer
            this.restServer = HugeRestServer.start(restServerConf);
        } catch (Throwable e) {
            LOG.error("HugeRestServer start error: ", e);
            try {
                this.gremlinServer.stop().get();
            } catch (Throwable t) {
                LOG.error("GremlinServer stop error: ", t);
            }
            HugeFactory.shutdown(30L);
            throw e;
        }
    }

    public void stop() {
        try {
            this.restServer.shutdown().get();
            LOG.info("HugeRestServer stopped");
        } catch (Throwable e) {
            LOG.error("HugeRestServer stop error: ", e);
        }

        try {
            this.gremlinServer.stop().get();
            LOG.info("HugeGremlinServer stopped");
        } catch (Throwable e) {
            LOG.error("HugeGremlinServer stop error: ", e);
        }

        try {
            HugeFactory.shutdown(30L);
            LOG.info("HugeGraph stopped");
        } catch (Throwable e) {
            LOG.error("Failed to stop HugeGraph: ", e);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            String msg = "HugeGraphServer can only accept two config files";
            LOG.error(msg);
            throw new HugeException(msg);
        }

        //PerfUtil.instance().profilePackage("com.baidu.hugegraph.");
        //PerfUtil.instance().profileClass("com.baidu.hugegraph.traversal" +
        //                                 "algorithm.ShortestPathTraverser$Traverser");
        //PerfUtil.instance().profileClass("com.baidu.hugegraph.backend.cache." +
        //                                 "CachedSchemaTransaction$SchemaCaches");

        HugeRestServer.register();

        HugeGraphServer server = new HugeGraphServer(args[0], args[1]);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("HugeGraphServer stopping");
            server.stop();
            LOG.info("HugeGraphServer stopped");
        }, "hugegraph-server-shutdown"));
    }
}
